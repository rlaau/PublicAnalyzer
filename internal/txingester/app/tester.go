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
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
)

const (
	ingestedBatchSize = 1000_000 // TxIngester가 한 번 요청에 받아온 값. 보통은 100개 정도겠지만, 극단성 위해서 1백만개로 설정
	sleepInterval     = 1 * time.Second
)

type TestingIngester struct {
	state             *IngestState
	minInterval       time.Duration
	transactionSource string // 트랜잭션 소스 (예: "test", "mainnet")
	kafkaClient       *kafka.Client
	cceService        txingester.CCEService // CCE 모듈과의 통신을 위한 서비스
	currentFileIndex  int                   // 현재 읽고 있는 파일 인덱스
	currentLineOffset int                   // 현재 파일 내 라인 오프셋
}
type IngestState struct {
	LastTxTime time.Time // 마지막 처리한 Tx의 시간

}

// NewTestingIngester returns initialized TestingIngester
func NewTestingIngester(kafkaClient *kafka.Client, cceService txingester.CCEService) *TestingIngester {
	return &TestingIngester{
		state:             &IngestState{},
		minInterval:       1 * time.Second,
		kafkaClient:       kafkaClient,
		cceService:        cceService,                                   // CCE 서비스를 의존성 주입으로 받음
		transactionSource: "/home/rlaaudgjs5638/chainAnalyzer/testdata", // 테스트 데이터 디렉토리
		currentFileIndex:  0,
		currentLineOffset: 0,
	}
}

func (app *TestingIngester) IngestTransaction(ctx context.Context) error {
	var (
		offset        int
		delay         = sleepInterval
		totalMessages int
	)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[IngestTransaction] Context canceled. Total messages sent: %d", totalMessages)
			return ctx.Err()
		default:
			// Kafka 상태 기반 delay 조절
			queueLength := app.mockQueueLength() // TODO 실제 consumer lag API 대체 필요
			delay = app.adjustDelay(queueLength)

			log.Printf("[IngestTransaction] Current offset: %d, Mock queue length: %d, Next delay: %v", offset, queueLength, delay)
		}
		// loadTestBatch가 이제 내부적으로 파일 인덱스와 라인 오프셋을 관리함
		rawTxs := app.loadTestBatch(offset, ingestedBatchSize)
		if len(rawTxs) == 0 {
			log.Printf("[IngestTransaction] No more data to load. Sleeping for %v", delay)
			time.Sleep(delay)
			continue
		}

		// CCE 모듈과 연계하여 올바른 트랜잭션 마킹 수행
		var miniBatch []kafka.Message[domain.MarkedTransaction]
		// 한번에 inject한 양을 보고 얼마만큼 카프카에 끊어서 보낼 지 결정
		var publishBatchSize = app.adjustBatchSize(ingestedBatchSize)
		for _, raw := range rawTxs {
			if app.CheckContractCreation(raw) {
				// 컨트렉트 생성 처리
				creator := domain.Address(safeBytes20(raw.From))
				nonce, _ := strconv.ParseUint(raw.Nonce, 10, 64)

				// CCE 모듈에 컨트렉트 등록하고 컨트렉트 주소 받기
				contractAddr, err := app.cceService.RegisterContract(creator, nonce, raw.BlockTime)
				if err != nil {
					log.Printf("Failed to register contract: %v", err)
					continue
				}

				// 컨트렉트 생성 트랜잭션을 마킹
				markedTx := domain.MarkedTransaction{
					BlockTime: raw.BlockTime,
					TxID:      app.stringToTxId(raw.TxId),
					From:      creator,
					To:        contractAddr,
					Value:     domain.NewBigInt(raw.Value),
					GasLimit:  domain.NewBigInt(raw.Gas),
					Input:     raw.Input,
					Nonce:     nonce,
					TxSyntax: [2]domain.ContractBoolMark{
						domain.EOAMark,      // Creator는 항상 EOA
						domain.ContractMark, // Target은 새로 생성된 컨트렉트
					},
				}

				miniBatch = append(miniBatch, kafka.Message[domain.MarkedTransaction]{
					Key:   []byte(raw.From),
					Value: markedTx,
				})
			} else {
				// 일반 트랜잭션 마킹해서 미니배치에 추가
				miniBatch = append(miniBatch, kafka.Message[domain.MarkedTransaction]{
					Key:   []byte(raw.From),
					Value: app.MarkTransaction(raw),
				})
			}
			//
			if len(miniBatch) >= publishBatchSize {
				err := kafka.PublishBatch(ctx, app.kafkaClient.Writer(), miniBatch)
				log.Printf("[IngestTransaction] Published batch of %d transactions", len(miniBatch))
				totalMessages += len(miniBatch)
				if err != nil {
					log.Printf("Failed to publish batch: %v", err)
					time.Sleep(delay)
					continue
				}

				miniBatch = miniBatch[:0] // miniBatch slice 재사용 (cap은 유지, len만 0으로)
			}
		}
		if len(miniBatch) > 0 {
			err := kafka.PublishBatch(ctx, app.kafkaClient.Writer(), miniBatch)
			if err != nil {
				log.Printf("Failed to publish batch: %v", err)
				time.Sleep(delay)
				continue
			}
		}
		log.Printf("[IngestTransaction] Total published so far: %d", totalMessages)

		offset += len(rawTxs)
		time.Sleep(delay)
	}
}

// CheckContractCreation checks if a transaction creates a new contract
// In Ethereum, contract creation happens when 'to' field is empty
func (app *TestingIngester) CheckContractCreation(rawTx domain.RawTransaction) bool {
	// Contract creation은 to 주소가 비어있을 때 발생
	return rawTx.To == "" || rawTx.To == "0x0000000000000000000000000000000000000000"
}

func (app *TestingIngester) MarkTransaction(rawTx domain.RawTransaction) domain.MarkedTransaction {
	fromAddr := domain.Address(safeBytes20(rawTx.From))
	toAddr := domain.Address(safeBytes20(rawTx.To))

	// Parse nonce from rawTx (default to 0 if not available)
	nonce, _ := strconv.ParseUint(rawTx.Nonce, 10, 64)

	markedTx := domain.MarkedTransaction{
		BlockTime: rawTx.BlockTime,
		TxID:      app.stringToTxId(rawTx.TxId),
		From:      fromAddr,
		To:        toAddr,
		Value:     domain.NewBigInt(rawTx.Value),
		GasLimit:  domain.NewBigInt(rawTx.Gas),
		Input:     rawTx.Input,
		Nonce:     nonce,
		TxSyntax: [2]domain.ContractBoolMark{
			app.CheckIsContract(fromAddr), // CCE 모듈을 통해 확인
			app.CheckIsContract(toAddr),   // CCE 모듈을 통해 확인
		},
	}
	return markedTx
}

func (app *TestingIngester) CheckIsContract(address domain.Address) domain.ContractBoolMark {
	if app.cceService.CheckIsContract(address) {
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
func (app *TestingIngester) stringToTxId(s string) domain.TxId {
	var txId domain.TxId
	copy(txId[:], []byte(s))
	return txId
}

// mockQueueLength provides a mock/stub queue length (can randomize or increment for demo)
func (app *TestingIngester) mockQueueLength() int {
	// Stub example: Always return 0 for now (replace with Kafka lag metric if needed)
	return 0
}

func (app *TestingIngester) adjustBatchSize(injestedBatchSize int) int {
	if injestedBatchSize > 1_000_000 {
		return 1_000_000 // 최대 1백만개로 제한
	}
	return injestedBatchSize
}
func (app *TestingIngester) adjustDelay(queueLength int) time.Duration {
	switch {
	case queueLength > 10_000_000:
		return 5 * time.Second
	case queueLength > 5_000_000:
		return 3 * time.Second
	case queueLength > 1_000_000:
		return 2 * time.Second
	default:
		return sleepInterval
	}
}

// loadTestBatch reads transactions from CSV files starting from the current file and line offset
func (app *TestingIngester) loadTestBatch(offset, limit int) []domain.RawTransaction {
	var txs []domain.RawTransaction
	remainingToLoad := limit

	for remainingToLoad > 0 {
		// 파일명 생성 (eth_tx_000000000000.csv 형식)
		fileName := fmt.Sprintf("eth_tx_%012d.csv", app.currentFileIndex)
		filePath := filepath.Join(app.transactionSource, fileName)

		// 파일이 존재하는지 확인
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			log.Printf("[loadTestBatch] No more files to read. Last file index: %d", app.currentFileIndex-1)
			break
		}

		// 파일 열기
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("[loadTestBatch] Error opening file %s: %v", filePath, err)
			app.currentFileIndex++
			app.currentLineOffset = 0
			continue
		}

		// CSV 리더 생성
		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1 // 가변 필드 수 허용 (데이터 오류 대응)
		reader.LazyQuotes = true
		reader.TrimLeadingSpace = true

		// 현재 라인까지 스킵
		for i := 0; i < app.currentLineOffset; i++ {
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
				app.currentFileIndex++
				app.currentLineOffset = 0
				break
			}
			if err != nil {
				log.Printf("[loadTestBatch] Error reading CSV: %v", err)
				app.currentLineOffset++
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
				BlockTime: time.Now(), // 테스트용 현재 시간
				TxId:      fmt.Sprintf("test_tx_%d_%d", app.currentFileIndex, app.currentLineOffset+linesRead),
				Value:     "0",
				Gas:       "21000",
				Input:     "",
			})
			remainingToLoad--

			linesRead++
		}

		app.currentLineOffset += linesRead
		file.Close()

		// 현재 파일에서 더 읽을 것이 없으면 다음 파일로
		if linesRead == 0 {
			app.currentFileIndex++
			app.currentLineOffset = 0
		}
	}

	log.Printf("[loadTestBatch] Loaded %d transactions from file index %d, line offset %d",
		len(txs), app.currentFileIndex, app.currentLineOffset)
	return txs
}
