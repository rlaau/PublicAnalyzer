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

	sb, err := dexfeeder.NewDexStreamBus(dexfeeder.DexStreamBusConfig{
		BufferSize:   100_000, // 버퍼(큐) 크기
		SaveInterval: time.Second,
	})
	if err != nil {
		log.Fatalf("StreamBus create error :%v", err)
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
	//TODO 최종적으론 서버 역시 Start에서 받고, ctx신호 받을 시 Start를  Close와 함께 리턴하기
	//TODO 보면 알겠지만 아래의 고루틴은 길기도 하지만 그게 문제가 아니라 제어가 더러움
	//TODO ctx로 깔끔하게 다뤄지는게 아니라, 그냥 고루틴 하나 방치해 둔 후 어떻게든 "탈출"만 가능하도록 만든 형태임
	//TODO 실제 서버 고루틴은 메인 루프 종료 후 강제 종료되는 형식. 좀비 스레드 될 확률도 있음.
	//TODO 여튼, 이는 임시방편일 뿐이란 것. 반드시 서버도 모듈화하기
	g.Go(func() error {
		fmt.Printf("   🌐 Starting API server on :8080\n")

		// 서버를 별도 고루틴에서 시작
		serverErrCh := make(chan error, 1)
		go func() {
			serverErrCh <- srv.Start()
		}()

		// ctx 신호 또는 서버 에러 대기
		select {
		case <-ctx.Done():
			fmt.Printf("서버: ctx 신호 수신 → 리턴\n")
			return nil
		case err := <-serverErrCh:
			fmt.Printf("서버 종료 완료\n")
			return err
		}
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

	//TODO 현재 StreamBus도 컨텍스트 관리 방식에 문제가 있음.
	//TODO 컨텍스트 전파는 되지만, 이후 컨텍스트 제어 신호의 핸들링이 부재함.
	//TODO 얘도 걍 Start로 받아서 전파 후, Start가 제어 신호 받을 때 Close호출하게 함으로써 제어하기
	sb.Start()
	g.Go(func() error {
		defer fmt.Printf("스트림 버스 종료 완료\n")
		if err := sb.StreamBetween(ctx,
			startTime.Format("2006-01-02"),
			endTime.Format("2006-01-02"),
		); err != nil {
			log.Printf("Stream error :%v", err)
			return err
		}
		return nil
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

	// 2. Server 종료 (HTTP 연결 정리)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("⚠️  서버 종료 중 에러: %v\n", err)
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
