package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	ap "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool"
	ainf "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod"
	co "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/app"
	eo "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/eo/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	rinf "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/infra"
	"golang.org/x/sync/errgroup"

	triplet "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/streamfeeder/bus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

const ProcessName = "Texting_Streaming"

func main() {
	root, cancle := context.WithCancel(context.Background())
	defer cancle()

	startTime, _ := time.Parse("2006-01-02", "2023-09-01")

	endTime, _ := time.Parse("2006-01-02", "2024-12-31")

	sb, err := bus.NewStreamBus(bus.StreamBusConfig{
		RootPathFunc:      func() string { return computation.FindProjectRootPath() },
		BusRelPath:        "streambus/rawtx.jsonl", // 원하면 경로 변경
		BufferSize:        100_000,                 // 버퍼(큐) 크기
		PositionDataDir:   computation.FindProductionStorageRootPath() + "/streambus_pos",
		SaveInterval:      time.Second,
		SortedStoragePath: computation.FindProjectRootPath() + "/stream_storage/sorted",
	})
	if err != nil {
		log.Fatalf("StreamBus create error :%v", err)
	}

	relPool := ComposeRelPool()
	nodPool := ComposeNodPool()
	apoolFanout := ainf.NewApoolFanoutManager("Testing_apool", 2000, 2000)
	//잠시 RelPool로 향하는 트랜잭션은 없앰
	apoolFanout.OffBus("RelBus")
	apool := ap.NewAnalyzerPool(mode.TestingModeProcess, apoolFanout, nodPool, relPool)

	fmt.Println("apool 성공적으로 초기화됨")
	g, ctx := errgroup.WithContext(root)
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
		fmt.Printf("시그널에 의해 종료\n")
	case <-ctx.Done():
		fmt.Printf("ctx의해 종료. %s", ctx.Err())
	}
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
		Name:           "Simple-Triplet",
		Mode:           mode.TestingModeProcess,
		BusCapLimit:    200_000,
		WorkerCount:    2,
		IsolatedDBPath: filepath.Join(computation.FindTestingStorageRootPath(), ProcessName),

		AutoCleanup:         false, // ←★ 결과 보존 위해 비활성화
		ResultReporting:     true,
		StatsInterval:       2_000_000_000, // 2초
		HealthCheckInterval: 3_000_000_000, // 3초
	}
	triplet, err := triplet.NewTriplet(&tripletConfig)
	if err != nil {
		panic("트리플렛 생성 중 에러")
	}
	relDistributor := rinf.NewRelFanoutManager("Testing_rel", 2000, 2000)
	relPool, _ := rel.NewRelPool(mode.TestingModeProcess, relDistributor, triplet)
	return relPool
}

func ComposeNodPool() *nod.NodPool {
	co := co.NewCo(co.CoCfg{
		Mode: mode.TestingModeProcess,
	})
	eo := eo.NewEo(mode.TestingModeProcess)
	nodDistributor := nod.NewNodPoolFanoutManager("Testing_nod", 2000, 2000)

	nodPool := nod.NewNodPool(mode.TestingModeProcess, nodDistributor, eo, co)
	return nodPool

}
