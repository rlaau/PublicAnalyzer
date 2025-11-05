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
	// 환경변수 설정.
	err := godotenv.Load(computation.GetENV())
	processMode := computation.TestingMode
	processName := computation.MustParseProcessName("streamable")
	computation.SetModeENV(processMode)
	computation.SetProcessNameENV(processName)
	if err != nil {
		fmt.Printf("환경변수 주입 실패.\n")
		fmt.Printf("그러나 도커 환경에선 정상 작동일 수 있음\n")
	}

	fmt.Printf("현재 초기화된 환경변수. 프로세스 모드 %s, 프로세스 이름 %s\n", os.Getenv(string(computation.PROCESS_MODE_KEY)), os.Getenv(string(computation.PROCESS_NAME_KEY)))

	//DexFeeder초기화
	startTime, _ := time.Parse("2006-01-02", "2023-09-01")
	endTime, _ := time.Parse("2006-01-02", "2024-12-31")
	streamTarget, isOk := dexfeeder.NewStreamTarget(startTime, endTime)
	if !isOk {
		panic("StreamTarget이 올바르게 파싱되지 못함.")
	}
	df, err := dexfeeder.NewDexFeeder(dexfeeder.DexFeederConfig{
		BufferSize:   100_000, // 버퍼(큐) 크기
		SaveInterval: time.Second,
		StreamTarget: streamTarget,
	})
	if err != nil {
		log.Panicf("StreamBus create error :%v", err)
	}

	//dexAnalyzerPool초기화
	relPool := ComposeRelPool()
	nodPool := ComposeNodPool()
	dxanFanout := ainf.NewDxanFanoutManager("Testing_apool", 2000, 2000)
	dexAnalyzerPool := ap.NewDexAnalyzerPool(mode.TestingModeProcess, dxanFanout, nodPool, relPool)
	fmt.Println("dxan 성공적으로 초기화됨")

	//server초기화
	srv := server.NewServer(":8080")
	srv.SetupBasicRoutes()
	relAPI := relapi.NewRelPoolAPIHandler(relPool)
	if err := srv.RegisterModule(relAPI); err != nil {
		fmt.Printf("서버에 Rel API등록 실패: %s\n", err)
	}

	//제어 컨텍스트 생성
	root, cancleCtx := context.WithCancel(context.Background())
	defer cancleCtx()
	g, ctx := errgroup.WithContext(root)

	// 초기화된 모듈들 Start
	g.Go(func() error {
		fmt.Printf("   🌐 Starting API server on :8080\n")
		err := srv.Start(ctx)
		fmt.Printf("server종료 완료\n")
		return err
	})
	g.Go(func() error {
		err := dexAnalyzerPool.Start(ctx)
		fmt.Printf("apool종료 완료\n")
		return err
	})
	g.Go(func() error {
		err := LoadAndTranslateToMarkedTransaction(df, dexAnalyzerPool, ctx)
		fmt.Printf("LoadTrans종료 완료")
		return err
	})
	g.Go(func() error {
		err := df.Start(ctx)
		fmt.Printf("DexFeeder종료 완료\n")
		return err
	})

	//os를 통한 별도의 제어 채널 형성.
	//최상위 루트에선 cmd나 ctrl+C로 제어 가능케 함
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// os 시그널을 통해서 제어 컨텍스트 접근 가능하게 하기
	go func() {
		<-sigChan
		fmt.Printf("\n🛑 시그널 수신 → 정리 작업 시작\n")
		cancleCtx()
	}()

	// 모든 고루틴 종료 대기
	if err := g.Wait(); err != nil {
		log.Printf("⚠️  종료 중 에러 발생: %v\n", err)
	}

	fmt.Printf("✅ 종료 완료\n")
}

func LoadAndTranslateToMarkedTransaction(
	sb *dexfeeder.DexFeeder,
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
			markedTx := parseMarkedTransaction(apool, rawTx)
			if err := apool.DispatchMarkedTx(ctx, markedTx); err != nil {
				log.Printf("apool에 markedTx 퍼블리시 중 에러%s\n", err.Error())
			}
			pubCount++
		}
	}

}

// parseMarkedTransaction는 RawTx->MarkedTx로 변환
func parseMarkedTransaction(apool *ap.DexAnalyzerPool, tx domain.RawTransaction) domain.MarkedTransaction {
	co := apool.GetNodPort().GetCoPort()
	from, _ := domain.ParseAddressFromString(tx.From)
	to, _ := domain.ParseAddressFromString(tx.To)
	fromIsContract := co.CheckIsContract(from)
	toIsContract := co.CheckIsContract(to)
	tid, _ := domain.ParseTxId(tx.TxId)
	txSyntax := [2]domain.ContractBoolMark{domain.ParseContractMark(fromIsContract), domain.ParseContractMark(toIsContract)}
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
	clusterDB, err := rinf.NewClusterDB(mode.TestingModeProcess)
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
