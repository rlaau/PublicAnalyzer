package app

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/txingester"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/ct"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
)

// TODO 이런것도 결국 상수 배제하기
// TODO 벡프레셔에 관리시키거나 할 것. 지금 이건 개-쓰레기 코드임
const (
	ingestedBatchSize = 1000                  // TxIngester가 한 번 요청에 받아온 값. 메모리 안정성을 위해 1000개로 설정
	sleepInterval     = 10 * time.Millisecond // 더 빠른 처리를 위해 간격 단축
	initialBatchSize  = 10000                 // 첫 프로듀싱 시 안전한 배치 크기

	// 큐 관리 상수들
	allowedQueueLimit      = 500000 // 허용 큐 한도 (500K 메시지) - 절대 초과하면 안되는 한계
	targetQueueSize        = 250000 // 목표 큐 크기 (250K 메시지, 허용 한도의 50%)
	criticalQueueThreshold = 400000 // 위험 임계값 (400K 메시지, 허용 한도의 80%)

	// 조정 상수들
	minInterval        = 1 * time.Second  // 최소 생성 간격
	maxInterval        = 10 * time.Second // 최대 생성 간격 (극한 백프레셔 시)
	minTPSThreshold    = 1000             // 최소 TPS 임계값 (초기화 시 0 TPS 문제 해결)
	backpressureWindow = 5                // 백프레셔 계산 윈도우 (초)

	// 조정 비율들
	aggressiveIncreaseRate = 1.5 // 큐가 여유로울 때 적극적 증가 비율
	normalIncreaseRate     = 1.1 // 정상 상황에서 점진적 증가 비율
	normalDecreaseRate     = 0.9 // 정상 상황에서 점진적 감소 비율
	aggressiveDecreaseRate = 0.6 // 큐가 위험할 때 적극적 감소 비율
)

type TestingIngester struct {
	//인제스터가 마지막으로 인제스트한 tx시간 기록
	state *IngestState
	// 타임아웃 용 최소 인터벌
	minInterval time.Duration
	//TODO 파일 읽는 용도-> 추후 state에 통합하기.
	//TODO 모드에 따라 움직이기. 파일 기반 or 스트리밍 기반이냐 따라 관리 상태 다름
	//TODO 모드 딸깍 하면 선택 적용시키기
	currentFileIndex  int // 현재 읽고 있는 파일 인덱스
	currentLineOffset int // 현재 파일 내 라인 오프셋
	//TODO 현재는 배치모드 뿐임. 근데 이제 배치와는 별개로 파일 처리도 분리.
	batchMode    bool          // 배치 모드 활성화 여부
	batchSize    int           // 배치 크기
	batchTimeout time.Duration // 배치 타임아웃

	// 백프레셔 관리를 위한 필드들
	currentInterval   time.Duration // 현재 생성 간격 (q)
	currentBatchCount int           // 현재 배치 크기 (p)
	firstProduction   bool          // 첫 프로듀싱 여부

	// 큐 상태 추적을 위한 추가 필드들
	//TODO 추후 얘내도 제거. 인제스터가 왜 큐 상태까지 보는것임. 백프레셔가 알아서 해라
	estimatedQueueSize float64 // 추정 큐 크기 (누적 계산)
	//얘는 벡프레셔의 "시간"이라서 실제 시간 사용
	lastUpdateTime      time.Time // 마지막 업데이트 시간
	smoothedProducerTPS float64   // 평활화된 Producer TPS
	smoothedConsumerTPS float64   // 평활화된 Consumer TPS

	//* 카프카 프로듀서, 동기 연결할 CCE서비스, 모니터링 등 정의
	infraStructure txingester.Infrastructure
}

type IngestState struct {
	LastTxTime ct.ChainTime // 마지막 처리한 Tx의 시간
}

// NewTestingIngester returns initialized TestingIngester (기본 모드)
func NewTestingIngester(infra txingester.Infrastructure) *TestingIngester {
	if infra.TransactionSource == "" {
		infra.TransactionSource = "/home/rlaaudgjs5638/chainAnalyzer/testdata"
	}

	return &TestingIngester{
		//TODO 추후 State가 "마지막 처리 시간을 로드 후 세이브"시켜야 함.
		//TODO 이건 현재 테스팅 인제스터라서 걍 디폴트 값으로 해 둔거임.
		//TODO 실전에서 이걸 세이브-로드하는 식으로 끝 값 기억후 그거 바탕 "타이머 이니셜라이즈"필요!!!
		state:             &IngestState{LastTxTime: ct.DefaultChainTime},
		minInterval:       1 * time.Second,
		currentFileIndex:  0,
		currentLineOffset: 0,
		batchMode:         false,                 // 기본값: 비배치 모드
		batchSize:         200,                   // 기본 배치 크기
		batchTimeout:      10 * time.Millisecond, // 기본 배치 타임아웃

		// 백프레셔 관리 초기화
		currentInterval:   minInterval,      // 최소 간격으로 시작
		currentBatchCount: initialBatchSize, // 첫 프로듀싱은 1만개
		firstProduction:   true,

		// 큐 상태 추적 초기화
		estimatedQueueSize:  0.0,
		lastUpdateTime:      time.Now(),
		smoothedProducerTPS: float64(minTPSThreshold), // 최소 TPS로 시작
		smoothedConsumerTPS: float64(minTPSThreshold), // 최소 TPS로 시작

		infraStructure: infra,
	}
}

// NewTestingIngesterWithBatch returns initialized TestingIngester with batch mode
func NewTestingIngesterWithBatch(infra txingester.Infrastructure, batchSize int, batchTimeout time.Duration) *TestingIngester {
	if infra.TransactionSource == "" {
		infra.TransactionSource = "/home/rlaaudgjs5638/chainAnalyzer/testdata"
	}
	return &TestingIngester{
		//TODO 추후 State가 "마지막 처리 시간을 로드 후 세이브"시켜야 함.
		//TODO 이건 현재 테스팅 인제스터라서 걍 디폴트 값으로 해 둔거임.
		//TODO 실전에서 이걸 세이브-로드하는 식으로 끝 값 기억후 그거 바탕 "타이머 이니셜라이즈"필요!!!
		state:             &IngestState{LastTxTime: ct.DefaultChainTime},
		minInterval:       1 * time.Second,
		currentFileIndex:  0,
		currentLineOffset: 0,
		batchMode:         true, // 배치 모드 활성화
		batchSize:         batchSize,
		batchTimeout:      batchTimeout,

		// 백프레셔 관리 초기화
		currentInterval:   minInterval,      // 최소 간격으로 시작
		currentBatchCount: initialBatchSize, // 첫 프로듀싱은 1만개
		firstProduction:   true,

		// 큐 상태 추적 초기화
		estimatedQueueSize:  0.0,
		lastUpdateTime:      time.Now(),
		smoothedProducerTPS: float64(minTPSThreshold), // 최소 TPS로 시작
		smoothedConsumerTPS: float64(minTPSThreshold), // 최소 TPS로 시작

		infraStructure: infra,
	}
}

// calculateBackpressure 백프레셔를 계산하여 현재 간격과 배치 크기를 조정
func (ti *TestingIngester) calculateBackpressure() {
	now := time.Now()
	timeDelta := now.Sub(ti.lastUpdateTime).Seconds()

	// 최소 업데이트 간격 보장 (너무 빈번한 조정 방지)
	if timeDelta < 1.0 {
		return
	}

	// 현재 TPS 값들 가져오기
	rawConsumerTPS := ti.infraStructure.KafkaMonitor.GetConsumerTPS()
	rawProducerTPS := ti.infraStructure.KafkaMonitor.GetProducerTPS()

	// 초기 TPS가 0인 경우 최소값으로 설정
	if rawConsumerTPS < float64(minTPSThreshold) {
		rawConsumerTPS = float64(minTPSThreshold)
	}
	if rawProducerTPS < float64(minTPSThreshold) {
		rawProducerTPS = float64(minTPSThreshold)
	}

	// TPS 값들을 평활화 (급격한 변화 방지)
	alpha := 0.3 // 평활화 계수 (0.3 = 30% 새 값, 70% 기존 값)
	ti.smoothedConsumerTPS = alpha*rawConsumerTPS + (1-alpha)*ti.smoothedConsumerTPS
	ti.smoothedProducerTPS = alpha*rawProducerTPS + (1-alpha)*ti.smoothedProducerTPS

	// 누적 큐 크기 계산 (Producer가 Consumer보다 빠를 때만 증가)
	netTPSDifference := ti.smoothedProducerTPS - ti.smoothedConsumerTPS
	ti.estimatedQueueSize += netTPSDifference * timeDelta

	// 큐 크기가 음수가 되지 않도록 보장
	if ti.estimatedQueueSize < 0 {
		ti.estimatedQueueSize = 0
	}

	// 큐 사용률 계산
	queueUsageRatio := ti.estimatedQueueSize / float64(allowedQueueLimit)
	targetUsageRatio := float64(targetQueueSize) / float64(allowedQueueLimit)          // 0.5 (50%)
	criticalUsageRatio := float64(criticalQueueThreshold) / float64(allowedQueueLimit) // 0.8 (80%)

	log.Printf("[Backpressure] Raw TPS - Consumer: %.2f, Producer: %.2f", rawConsumerTPS, rawProducerTPS)
	log.Printf("[Backpressure] Smoothed TPS - Consumer: %.2f, Producer: %.2f", ti.smoothedConsumerTPS, ti.smoothedProducerTPS)
	log.Printf("[Backpressure] Estimated queue size: %.0f, Usage ratio: %.2f%%, Target: %.2f%%",
		ti.estimatedQueueSize, queueUsageRatio*100, targetUsageRatio*100)

	// 첫 프로듀싱은 안전하게 시작
	if ti.firstProduction {
		ti.currentBatchCount = initialBatchSize
		ti.currentInterval = minInterval
		ti.firstProduction = false
		ti.lastUpdateTime = now
		log.Printf("[Backpressure] First production: batch=%d, interval=%v", ti.currentBatchCount, ti.currentInterval)
		return
	}

	// 백프레셔 조정 로직
	switch {
	case queueUsageRatio >= 1.0: // 100% 이상 - 긴급 상황
		ti.currentBatchCount = int(float64(ti.currentBatchCount) * aggressiveDecreaseRate * 0.5) // 더 강한 감소
		ti.currentInterval = time.Duration(float64(ti.currentInterval) * 2.0)                    // 간격 2배 증가
		log.Printf("[Backpressure] EMERGENCY: Queue overflow! Aggressive reduction")

	case queueUsageRatio >= criticalUsageRatio: // 80% 이상 - 위험
		ti.currentBatchCount = int(float64(ti.currentBatchCount) * aggressiveDecreaseRate)
		ti.currentInterval = time.Duration(float64(ti.currentInterval) * 1.4)
		log.Printf("[Backpressure] CRITICAL: Queue at %.1f%% - aggressive decrease", queueUsageRatio*100)

	case queueUsageRatio > targetUsageRatio*1.2: // 목표치의 120% 이상 - 점진적 감소
		ti.currentBatchCount = int(float64(ti.currentBatchCount) * normalDecreaseRate)
		ti.currentInterval = time.Duration(float64(ti.currentInterval) * 1.1)
		log.Printf("[Backpressure] HIGH: Queue at %.1f%% - gradual decrease", queueUsageRatio*100)

	case queueUsageRatio < targetUsageRatio*0.8: // 목표치의 80% 미만 - 점진적 증가
		ti.currentBatchCount = int(float64(ti.currentBatchCount) * normalIncreaseRate)
		if ti.currentInterval > minInterval {
			ti.currentInterval = time.Duration(float64(ti.currentInterval) * 0.95)
		}
		log.Printf("[Backpressure] LOW: Queue at %.1f%% - gradual increase", queueUsageRatio*100)

	case queueUsageRatio < targetUsageRatio*0.5: // 목표치의 50% 미만 - 적극적 증가
		ti.currentBatchCount = int(float64(ti.currentBatchCount) * aggressiveIncreaseRate)
		if ti.currentInterval > minInterval {
			ti.currentInterval = time.Duration(float64(ti.currentInterval) * 0.9)
		}
		log.Printf("[Backpressure] VERY LOW: Queue at %.1f%% - aggressive increase", queueUsageRatio*100)

	default: // 목표치 근처 - 유지
		log.Printf("[Backpressure] OPTIMAL: Queue at %.1f%% - maintaining current settings", queueUsageRatio*100)
	}

	// 경계값 적용
	if ti.currentBatchCount < minTPSThreshold {
		ti.currentBatchCount = minTPSThreshold
	}
	if ti.currentBatchCount > 50000 { // 최대 배치 크기 제한
		ti.currentBatchCount = 50000
	}
	if ti.currentInterval < minInterval {
		ti.currentInterval = minInterval
	}
	if ti.currentInterval > maxInterval {
		ti.currentInterval = maxInterval
	}

	ti.lastUpdateTime = now

	log.Printf("[Backpressure] Final settings: batch=%d, interval=%v", ti.currentBatchCount, ti.currentInterval)
}

// IngestTransaction 간단한 트랜잭션 수집 함수
func (ti *TestingIngester) IngestTransaction(ctx context.Context) error {
	var totalMessages int

	log.Printf("[IngestTransaction] Starting transaction ingestion")

	// 간단한 for 루프
	for {
		// Context 취소 확인
		select {
		case <-ctx.Done():
			log.Printf("[IngestTransaction] Context canceled. Total messages sent: %d", totalMessages)
			return ctx.Err()
		default:
		}

		// 백프레셔 계산 및 조정
		ti.calculateBackpressure()

		// 동적 배치 크기로 데이터 읽기
		rawTxs := ti.loadTestBatch(0, ti.currentBatchCount)
		if len(rawTxs) == 0 {
			log.Printf("[IngestTransaction] No more data to load. Waiting...")
			time.Sleep(1 * time.Second)
			continue
		}

		// CCE 모듈과 연계하여 올바른 트랜잭션 마킹 수행
		var markedTxs []domain.MarkedTransaction

		for _, raw := range rawTxs {
			//* 인제스트 하면서 시간을 전파하는 대목
			ct.AdvanceTo(ct.ParseEthTime(raw.BlockTime))
			var markedTx domain.MarkedTransaction

			if ti.CheckContractCreation(raw) {
				// 컨트렉트 생성 처리
				creator := domain.Address(safeBytes20(raw.From))
				nonce, _ := strconv.ParseUint(raw.Nonce, 10, 64)

				// CCE 모듈에 컨트렉트 등록하고 컨트렉트 주소 받기
				contractAddr, err := ti.infraStructure.CCEService.RegisterContract(creator, nonce, ct.ParseEthTime(raw.BlockTime))
				if err != nil {
					log.Printf("Failed to register contract: %v", err)
					continue
				}

				// 컨트렉트 생성 트랜잭션을 마킹
				markedTx = domain.MarkedTransaction{
					BlockTime: ct.ParseEthTime(raw.BlockTime),
					TxID:      ti.stringToTxId(raw.TxId),
					From:      creator,
					To:        contractAddr,
					Nonce:     nonce,
					TxSyntax: [2]domain.ContractBoolMark{
						domain.EOAMark,      // Creator는 항상 EOA
						domain.ContractMark, // Target은 새로 생성된 컨트렉트
					},
				}
			} else {
				// 일반 트랜잭션 마킹
				markedTx = ti.MarkTransaction(raw)
			}

			markedTxs = append(markedTxs, markedTx)
		}

		// 배치 또는 개별 전송 (모드에 따라 선택)
		if ti.batchMode && ti.infraStructure.BatchProducer != nil {
			// 배치 전송 모드
			if err := ti.publishBatch(ctx, rawTxs, markedTxs); err != nil {
				log.Printf("Failed to publish batch: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			// Producer TPS 모니터링 - 배치 단위로 기록
			ti.infraStructure.KafkaMonitor.RecordProducerTpsEvents(len(markedTxs))
			totalMessages += len(markedTxs)
			log.Printf("[IngestTransaction] Batch published %d transactions (total: %d)", len(markedTxs), totalMessages)
		} else {
			// 개별 전송 모드
			var successCount = 0

			for i, markedTx := range markedTxs {
				key := []byte(rawTxs[i].From)
				if err := ti.infraStructure.KafkaProducer.PublishMessage(ctx, key, markedTx); err != nil {
					log.Printf("Failed to publish transaction: %v", err)
					continue
				}
				// Producer TPS 모니터링 - 개별 메시지마다 기록
				ti.infraStructure.KafkaMonitor.RecordProducerTpsEvent()
				successCount++
			}

			totalMessages += successCount
			log.Printf("[IngestTransaction] Published %d/%d transactions (total: %d)", successCount, len(markedTxs), totalMessages)
		}

		// 동적 간격으로 대기
		time.Sleep(ti.currentInterval)
	}
}

// simulateConsumerReading Consumer 읽기를 시뮬레이션하여 백프레셔 계산용 TPS 측정
// 실제 환경에서는 downstream consumer에서 직접 호출됨
func (ti *TestingIngester) simulateConsumerReading(ctx context.Context, topic string, groupID string) error {
	// 실제 환경에서는 Consumer가 별도 모듈에서 실행됨
	consumer := kafka.NewKafkaConsumer[domain.MarkedTransaction](nil, topic, groupID)
	defer consumer.Close()

	log.Printf("[Consumer Simulator] Starting consumer simulation for topic: %s", topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Consumer Simulator] Context canceled")
			return ctx.Err()
		default:
		}

		// Consumer의 ReadMessage 호출 지점에서 TPS 모니터링
		_, _, err := consumer.ReadMessage(ctx)
		if err != nil {
			// 메시지가 없거나 오류 발생 시 잠시 대기
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Consumer TPS 모니터링 - ReadMessage 성공 시마다 기록
		ti.infraStructure.KafkaMonitor.RecordConsumerTpsEvent()
	}
}

// RunWithBackpressure 백프레셔 시스템과 함께 실행하는 통합 메서드
func (ti *TestingIngester) RunWithBackpressure(ctx context.Context, topic string, consumerGroupID string) error {
	// Consumer 시뮬레이션을 별도 고루틴에서 실행
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	go func() {
		if err := ti.simulateConsumerReading(consumerCtx, topic, consumerGroupID); err != nil {
			log.Printf("[Consumer Simulator] Error: %v", err)
		}
	}()

	// 잠시 대기하여 Consumer가 시작되도록 함
	time.Sleep(2 * time.Second)

	// Producer(Ingester) 실행
	return ti.IngestTransaction(ctx)
}

// CheckContractCreation checks if a transaction creates a new contract
// In Ethereum, contract creation happens when 'to' field is empty
func (ti *TestingIngester) CheckContractCreation(rawTx domain.RawTransaction) bool {
	// Contract creation은 to 주소가 비어있을 때 발생
	return rawTx.To == "" || rawTx.To == "0x0000000000000000000000000000000000000000"
}

func (ti *TestingIngester) MarkTransaction(rawTx domain.RawTransaction) domain.MarkedTransaction {
	fromAddr := domain.Address(safeBytes20(rawTx.From))
	toAddr := domain.Address(safeBytes20(rawTx.To))

	// Parse nonce from rawTx (default to 0 if not available)
	nonce, _ := strconv.ParseUint(rawTx.Nonce, 10, 64)
	chainTime, err := ct.ParseRFC3339(rawTx.BlockTime)
	if err != nil {
		panic("현재 파싱 로직이 올바르지 않음. TestingIngester가 MarkTransaction과정에서 시간을 파싱하지 못함")
	}
	markedTx := domain.MarkedTransaction{
		BlockTime: chainTime,
		TxID:      ti.stringToTxId(rawTx.TxId),
		From:      fromAddr,
		To:        toAddr,
		Nonce:     nonce,
		TxSyntax: [2]domain.ContractBoolMark{
			ti.CheckIsContract(fromAddr), // CCE 모듈을 통해 확인
			ti.CheckIsContract(toAddr),   // CCE 모듈을 통해 확인
		},
	}
	return markedTx
}

func (ti *TestingIngester) CheckIsContract(address domain.Address) domain.ContractBoolMark {
	if ti.infraStructure.CCEService.CheckIsContract(address) {
		return domain.ContractMark
	}
	return domain.EOAMark
}

// safeBytes20 ensures input string is padded/truncated safely to 20 bytes
func safeBytes20(s string) [20]byte {
	var arr [20]byte
	copy(arr[:], []byte(s))
	return arr
}

// stringToTxId converts string to TxId (32 bytes)
func (ti *TestingIngester) stringToTxId(s string) domain.TxId {
	var txId domain.TxId
	copy(txId[:], []byte(s))
	return txId
}

// loadTestBatch reads transactions from CSV files starting from the current file and line offset
func (ti *TestingIngester) loadTestBatch(_ int, limit int) []domain.RawTransaction {
	var txs []domain.RawTransaction
	remainingToLoad := limit

	for remainingToLoad > 0 {
		// 파일명 생성 (eth_tx_000000000000.csv 형식)
		fileName := fmt.Sprintf("eth_tx_%012d.csv", ti.currentFileIndex)
		filePath := filepath.Join(ti.infraStructure.TransactionSource, fileName)

		// 파일이 존재하는지 확인
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			log.Printf("[loadTestBatch] No more files to read. Last file index: %d", ti.currentFileIndex-1)
			break
		}

		// 파일 열기
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("[loadTestBatch] Error opening file %s: %v", filePath, err)
			ti.currentFileIndex++
			ti.currentLineOffset = 0
			continue
		}

		// CSV 리더 생성
		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1 // 가변 필드 수 허용 (데이터 오류 대응)
		reader.LazyQuotes = true
		reader.TrimLeadingSpace = true

		// 현재 라인까지 스킵
		for i := 0; i < ti.currentLineOffset; i++ {
			_, err := reader.Read()
			if err != nil {
				break
			}
		}

		// 트랜잭션 읽기
		linesRead := 0
		for remainingToLoad > 0 {
			record, err := reader.Read()
			if err == io.EOF {
				// 파일 끝에 도달, 다음 파일로
				ti.currentFileIndex++
				ti.currentLineOffset = 0
				break
			}
			if err != nil {
				log.Printf("[loadTestBatch] Error reading CSV: %v", err)
				ti.currentLineOffset++
				linesRead++
				continue
			}

			from, to := "", ""
			// 열이 2개 이상인 경우만 유효한 트랜잭션으로 처리
			if len(record) >= 2 {
				from = strings.TrimSpace(record[0])
				to = strings.TrimSpace(record[1])
			} else {
				from = strings.TrimSpace(record[0])
				to = "0x0000000000000000000000000000000000000000" // to가 없는 경우, 컨트렉트 생성 트랜잭션임

			}
			txs = append(txs, domain.RawTransaction{
				From:      from,
				To:        to,
				BlockTime: time.Now().Format(time.RFC3339), // 테스트용 현재 시간
				TxId:      fmt.Sprintf("test_tx_%d_%d", ti.currentFileIndex, ti.currentLineOffset+linesRead),
			})
			remainingToLoad--

			linesRead++
		}

		ti.currentLineOffset += linesRead
		file.Close()

		// 현재 파일에서 더 읽을 것이 없으면 다음 파일로
		if linesRead == 0 {
			ti.currentFileIndex++
			ti.currentLineOffset = 0
		}
	}

	log.Printf("[loadTestBatch] Loaded %d transactions from file index %d, line offset %d",
		len(txs), ti.currentFileIndex, ti.currentLineOffset)
	return txs
}

// publishBatch 배치 전송을 위한 헬퍼 메서드
func (ti *TestingIngester) publishBatch(ctx context.Context, rawTxs []domain.RawTransaction, markedTxs []domain.MarkedTransaction) error {
	if len(rawTxs) != len(markedTxs) {
		return fmt.Errorf("rawTxs and markedTxs length mismatch: %d vs %d", len(rawTxs), len(markedTxs))
	}

	// Message 배치 생성
	messages := make([]kafka.Message[domain.MarkedTransaction], len(markedTxs))
	for i, markedTx := range markedTxs {
		messages[i] = kafka.Message[domain.MarkedTransaction]{
			Key:   []byte(rawTxs[i].From),
			Value: markedTx,
		}
	}

	// 배치 전송
	return ti.infraStructure.BatchProducer.PublishMessagesBatch(ctx, messages)
}
