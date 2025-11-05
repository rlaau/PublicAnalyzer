package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	ap "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan"
	ainf "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/infra"
	np "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/app"
	co "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/co/app"
	eo "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/eo/app"
	ninf "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel"
	relapi "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/api"
	rinf "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/server"
	"golang.org/x/sync/errgroup"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dexfeeder"
	triplet "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/app"
	tinf "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

const ProcessName = "Testing_Streaming"

func main() {
	// Docker 환경에서는 환경변수가 이미 주입되어 있으므로 .env 로드 생략 가능
	err := godotenv.Load(computation.GetENV())
	if err != nil {
		fmt.Printf("환경변수 주입 실패.\n")
		fmt.Printf("그러나 도커 환경에선 정상 작동일 수 있음")
	}

	root, cancle := context.WithCancel(context.Background())
	defer cancle()

	startTime, _ := time.Parse("2006-01-02", "2023-09-01")
	endTime, _ := time.Parse("2006-01-02", "2024-12-31")
	streamTarget, isOk := dexfeeder.NewStreamTarget(startTime, endTime)
	if !isOk {
		panic("StreamTarget이 올바르게 파싱되지 못함.")
	}
	sb, err := dexfeeder.NewDexStreamBus(dexfeeder.DexStreamBusConfig{
		BufferSize:   100_000, // 버퍼(큐) 크기
		SaveInterval: time.Second,
		StreamTarget: streamTarget,
	})
	if err != nil {
		log.Panicf("StreamBus create error :%v", err)
	}

	relPool := ComposeRelPool()
	nodPool := ComposeNodPool()
	apoolFanout := ainf.NewDxanFanoutManager("Testing_apool", 2000, 2000)

	apool := ap.NewDexAnalyzerPool(mode.TestingModeProcess, apoolFanout, nodPool, relPool)

	fmt.Println("apool 성공적으로 초기화됨")

	srv := server.NewServer(":8080")
	srv.SetupBasicRoutes()
	relAPI := relapi.NewRelPoolAPIHandler(relPool)
	if err := srv.RegisterModule(relAPI); err != nil {
		fmt.Printf("서버에 Rel API등록 실패: %s\n", err)
	}

	g, ctx := errgroup.WithContext(root)

	g.Go(func() error {
		fmt.Printf("   🌐 Starting API server on :8080\n")
		err := srv.Start(ctx)
		fmt.Printf("server종료 완료\n")
		return err
	})
	g.Go(func() error {
		err := apool.Start(ctx)
		fmt.Printf("apool종료 완료\n")
		return err
	})
	g.Go(func() error {
		err := LoadAndTranslateToMarkedTransaction(sb, apool, ctx)
		fmt.Printf("LoadTrans종료 완료")
		return err
	})
	g.Go(func() error {
		err := sb.Start(ctx)
		fmt.Printf("DexFeeder종료 완료\n")
		return err
	})
	//특수 제어 목적
	//최상위 루트에선 cmd나 ctrl+C로 제어 가능케 함
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 시그널 대기
	go func() {
		<-sigChan
		fmt.Printf("\n🛑 시그널 수신 → 정리 작업 시작\n")
		cancle()
	}()

	// 모든 고루틴 종료 대기
	if err := g.Wait(); err != nil {
		log.Printf("⚠️  종료 중 에러 발생: %v\n", err)
	}

	fmt.Printf("✅ 종료 완료\n")
}

func LoadAndTranslateToMarkedTransaction(
	sb *dexfeeder.DexStreamBus,
	apool *ap.DexAnalyzerPool,
	ctx context.Context,
) error {
	pubCount := 0
	log.Printf("RawTx->MarkedTx변환 고루틴 시작")

	statusTicker := time.NewTicker(10 * time.Second)
	defer statusTicker.Stop()

	rawInputBus := sb.Bus().Dequeue()
	for {
		select {
		case <-ctx.Done():
			log.Printf("ETL종료 by context canceled\n")
			return nil
		case <-statusTicker.C:
			log.Printf("📊 [Bus Status] RawInputBus - len: %d, cap: %d |현재 %d개 퍼블리시",
				len(rawInputBus), cap(rawInputBus), pubCount)
		case rawTx, ok := <-rawInputBus:
			if !ok {
				log.Printf("RawInputBus가 닫힘 - ETL 종료\n")
				return nil
			}
			markedTx := translateTx(apool, rawTx)
			if err := apool.DispatchMarkedTx(ctx, markedTx); err != nil {
				log.Printf("apool에 markedTx 퍼블리시 중 에러%s\n", err.Error())
			}
			pubCount++
		}
	}

}

func translateTx(apool *ap.DexAnalyzerPool, tx domain.RawTransaction) domain.MarkedTransaction {
	co := apool.GetNodPort().GetCoPort()
	from, _ := domain.ParseAddressFromString(tx.From)
	to, _ := domain.ParseAddressFromString(tx.To)
	fromIsContract := co.CheckIsContract(from)
	toIsContract := co.CheckIsContract(to)
	tid, _ := domain.ParseTxId(tx.TxId)
	txSyntax := [2]domain.ContractBoolMark{changeBoolToContractMark(fromIsContract), changeBoolToContractMark(toIsContract)}
	markedTx := domain.MarkedTransaction{
		TxID:        tid,
		TxSyntax:    txSyntax,
		Nonce:       tx.Nonce,
		BlockTime:   chaintimer.ParseEthTime(tx.BlockTime),
		From:        from,
		To:          to,
		ValueOrNull: domain.ParseValueOption(tx.ValueOrNull),
	}
	return markedTx
}

func changeBoolToContractMark(isContract bool) domain.ContractBoolMark {
	if isContract {
		return domain.ContractMark
	}
	return domain.EOAMark
}
func ComposeRelPool() *rel.RelationPool {
	tripletConfig := triplet.TripletConfig{
		Name: "Simple-Triplet",
		Mode: mode.TestingModeProcess,
	}
	tripletInfraConfig := tinf.TripletInfraConfig{
		Mode:        mode.TestingModeProcess,
		WorkerCount: 2,
	}
	tripletInfra := tinf.NewTripletInfraByConfig(tripletInfraConfig)
	triplet, err := triplet.NewTripletAnalyzer(tripletConfig, tripletInfra)
	if err != nil {
		panic("트리플렛 생성 중 에러")
	}
	relDistributor := rinf.NewRelFanoutManager("Testing_rel", 2000, 2000)
	//TODO 이 코드 추후 수정하기.
	//TODO 로프DB의 구조 자체를 워커풀 & 스타트 기반으로 바꿀 것.
	//TODO 지금 콘텍스트 주는건 그냥 끼워맞추기 용이고, 실제 수정본에선 New시엔 컨텍스트 x였다가
	//TODO Start에서 ctx전파 받기
	clusterDB, err := rinf.NewClusterDB(context.Background(), mode.TestingModeProcess)
	if err != nil {
		fmt.Printf("ComposeRelPool서 clusterDB생성 중 에러 발생. %s\n", err.Error())
		return nil
	}
	relPool, _ := rel.NewRelPool(mode.TestingModeProcess, relDistributor, clusterDB, triplet)
	return relPool
}

func ComposeNodPool() *np.NodPool {
	co := co.NewCo(co.CoCfg{
		Mode: mode.TestingModeProcess,
	})
	eo := eo.NewEo(mode.TestingModeProcess)
	nodDistributor := ninf.NewNodPoolFanoutManager("Testing_nod", 2000, 2000)

	nodPool := np.NewNodPool(mode.TestingModeProcess, nodDistributor, eo, co)
	return nodPool

}
