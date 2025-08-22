package main

import (
	"context"
	"fmt"
	"log"
	"time"

	ap "github.com/rlaaudgjs5638/chainAnalyzer/shared/analyzerpool/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/analyzerpool/dto"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// 포트 인터페이스는 프로젝트에서 정의한다고 했으니,
// 여기서는 간단한 읽기 메서드를 가진 예시 구현만 둡니다.
type CCPort interface {
	GetShardStat(id string) string
}
type ccPortImpl struct{}

func (cc *ccPortImpl) GetShardStat(id string) string { return "OK:" + id }

func main() {
	// 1) 풀 생성 (테스트 모드)
	var isTest mode.ProcessingMode = mode.TestingModeProcess
	pool, err := ap.CreateAnalyzerPoolFrame(isTest)
	if err != nil {
		log.Fatalf("create pool: %v", err)
	}
	defer pool.Close(context.Background())

	// 2) 포트 주입 (cc 포트만 예시 구현)
	pool.Register(&ccPortImpl{}, nil, nil, nil)

	// 3) 읽기 포트 사용 (동기 메서드 콜)
	ccView := pool.GetViewCC().(CCPort) // 실제 포트 인터페이스 타입으로 캐스팅
	fmt.Println("CC shard stat:", ccView.GetShardStat("shard-3"))

	// 4) CC 이벤트 소비 루프 (CC 모듈 측)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			select {
			case evt, ok := <-pool.DequeueCC():
				if !ok {
					fmt.Println("[CC] dequeue closed")
					return
				}
				fmt.Printf("[CC] handle event: %+v\n", evt)
				// CC가 커맨드를 처리...
			case <-ctx.Done():
				return
			}
		}
	}()

	// 5) EE → CC로 커맨드 전송 (이벤트 기반 통신)
	if err := pool.PublishToCC(dto.CcEvt{Cmd: "rebuild", Arg: "shard-3"}); err != nil {
		log.Printf("publish CC: %v", err)
	} else {
		fmt.Printf("퍼블리시 성공")
	}

	// // 6) Tx 소비 예시 (CC 전용)
	// go func() {
	// 	for tx := range pool.ConsumeCCTx() {
	// 		fmt.Printf("[CC] consume tx: %+v\n", tx)
	// 	}
	// }()

	// 데모용 대기
	time.Sleep(800 * time.Millisecond)
}
