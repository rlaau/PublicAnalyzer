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
	ap "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool"
	ainf "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/infra"
	np "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/app"
	co "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/app"
	eo "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/eo/app"
	ninf "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	relapi "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/api"
	rinf "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/server"
	"golang.org/x/sync/errgroup"

	triplet "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	tinf "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/streamfeeder/bus"
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

	sb, err := bus.NewStreamBus(bus.StreamBusConfig{
		BufferSize:   100_000, // 버퍼(큐) 크기
		SaveInterval: time.Second,
	})
	if err != nil {
		log.Fatalf("StreamBus create error :%v", err)
	}

	relPool := ComposeRelPool()
	nodPool := ComposeNodPool()
	apoolFanout := ainf.NewApoolFanoutManager("Testing_apool", 2000, 2000)

	apool := ap.NewAnalyzerPool(mode.TestingModeProcess, apoolFanout, nodPool, relPool)

	fmt.Println("apool 성공적으로 초기화됨")

	srv := server.NewServer(":8080")
	srv.SetupBasicRoutes()
	relAPI := relapi.NewRelPoolAPIHandler(relPool)
	if err := srv.RegisterModule(relAPI); err != nil {
		fmt.Printf("서버에 Rel API등록 실패: %s\n", err)
	}

	g, ctx := errgroup.WithContext(root)
	g.Go(func() error { fmt.Printf("   🌐 Starting API server on :8080\n"); return srv.Start() })
	g.Go(func() error { return apool.Start(ctx) })
	g.Go(func() error { return LoadAndTranslateToMarkedTransaction(sb, apool, ctx) })
	sb.Start()
	g.Go(func() error {
		if err := sb.StreamBetween(
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
	select {
	case <-sigChan:
		fmt.Printf("시그널에 수신-> 종료 시작\n")
		cancle()
	case <-ctx.Done():
		fmt.Printf("ctx의해 종료. %s", ctx.Err())
	}
	fmt.Printf("종료 완료")
}

func LoadAndTranslateToMarkedTransaction(
	sb *bus.StreamBus,
	apool *ap.AnalyzerPool,
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
			log.Printf("ETL종료 by context candled\n")
		case <-statusTicker.C:
			log.Printf("📊 [Bus Status] RawInputBus - len: %d, cap: %d |현재 %d개 퍼블리시",
				len(rawInputBus), cap(rawInputBus), pubCount)
		case rawTx, ok := <-rawInputBus:
			if !ok {
				log.Printf("RawInoutBus가 제대로 제대로 메시지 전달 못함")
			}
			markedTx := translateTx(apool, rawTx)
			if err := apool.DispatchMarkedTx(markedTx); err != nil {
				log.Printf("apool에 markedTx 퍼블리시 중 에러%s\n", err.Error())
			}
			pubCount++
		}
	}

}

func translateTx(apool *ap.AnalyzerPool, tx domain.RawTransaction) domain.MarkedTransaction {
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
		ValueOrNull: tx.ValueOrNull,
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
	relPool, _ := rel.NewRelPool(mode.TestingModeProcess, relDistributor, triplet)
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
