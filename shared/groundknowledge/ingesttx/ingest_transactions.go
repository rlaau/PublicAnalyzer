package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	// BigQuery 설정
	projectID = "whaletracker-453817"

	// 병렬 처리 설정
	maxGoroutines = 25 // 최대 25개 고루틴 (1주일치씩)
	daysPerBatch  = 7  // 배치당 일수

	// 파일 저장 설정
	dataDir       = "/home/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/ingesttx/data"
	csvFilePrefix = "ethereum_tx_"
)

// BigQueryTxRow BigQuery에서 가져오는 최소한의 트랜잭션 데이터
type BigQueryTxRow struct {
	Hash           string    `bigquery:"hash"`
	BlockTimestamp time.Time `bigquery:"block_timestamp"`
	FromAddress    string    `bigquery:"from_address"`
	ToAddress      string    `bigquery:"to_address"`
	Nonce          int64     `bigquery:"nonce"`
}

// ParallelTransactionIngester 병렬로 트랜잭션을 가져오는 구조체
type ParallelTransactionIngester struct {
	projectID string
	startDate string
	endDate   string
}

// NewParallelTransactionIngester 새로운 병렬 ingester 생성
func NewParallelTransactionIngester(projectID, startDate, endDate string) *ParallelTransactionIngester {
	return &ParallelTransactionIngester{
		projectID: projectID,
		startDate: startDate,
		endDate:   endDate,
	}
}

// IngestTransactionsParallel 병렬로 트랜잭션 데이터 수집
func (pti *ParallelTransactionIngester) IngestTransactionsParallel() error {
	// 데이터 디렉토리 생성
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("데이터 디렉토리 생성 실패: %v", err)
	}

	// 날짜 범위를 주 단위로 분할
	dateRanges, err := pti.createDateRanges()
	if err != nil {
		return fmt.Errorf("날짜 범위 생성 실패: %v", err)
	}

	log.Printf("총 %d개 배치로 병렬 처리 시작 (배치당 %d일)", len(dateRanges), daysPerBatch)

	// 병렬 처리용 채널과 WaitGroup
	sem := make(chan struct{}, maxGoroutines) // 동시 실행 제한
	var wg sync.WaitGroup
	errChan := make(chan error, len(dateRanges))

	// 각 날짜 범위를 병렬로 처리
	for i, dateRange := range dateRanges {
		wg.Add(1)

		go func(batchNum int, startDate, endDate string) {
			defer wg.Done()

			// 동시 실행 제한
			sem <- struct{}{}
			defer func() { <-sem }()

			log.Printf("배치 %d 시작: %s ~ %s", batchNum, startDate, endDate)

			if err := pti.processBatch(batchNum, startDate, endDate); err != nil {
				errChan <- fmt.Errorf("배치 %d 실패: %v", batchNum, err)
				return
			}

			log.Printf("배치 %d 완료: %s ~ %s", batchNum, startDate, endDate)
		}(i, dateRange.Start, dateRange.End)
	}

	// 모든 고루틴 완료 대기
	wg.Wait()
	close(errChan)

	// 에러 체크
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	log.Printf("모든 배치 처리 완료!")
	return nil
}

// DateRange 날짜 범위 구조체
type DateRange struct {
	Start string
	End   string
}

// createDateRanges 시작일부터 종료일까지 주 단위로 날짜 범위 생성
func (pti *ParallelTransactionIngester) createDateRanges() ([]DateRange, error) {
	startTime, err := time.Parse("2006-01-02", pti.startDate)
	if err != nil {
		return nil, fmt.Errorf("시작일 파싱 실패: %v", err)
	}

	endTime, err := time.Parse("2006-01-02", pti.endDate)
	if err != nil {
		return nil, fmt.Errorf("종료일 파싱 실패: %v", err)
	}

	var ranges []DateRange
	current := startTime

	for current.Before(endTime) {
		rangeEnd := current.AddDate(0, 0, daysPerBatch)
		if rangeEnd.After(endTime) {
			rangeEnd = endTime
		}

		ranges = append(ranges, DateRange{
			Start: current.Format("2006-01-02"),
			End:   rangeEnd.Format("2006-01-02"),
		})

		current = rangeEnd
	}

	return ranges, nil
}

// processBatch 단일 배치 처리
func (pti *ParallelTransactionIngester) processBatch(batchNum int, startDate, endDate string) error {
	ctx := context.Background()

	// BigQuery 클라이언트 생성 (배치별로 별도 클라이언트, 서비스 계정 사용)
	credentialsFile := "/home/rlaaudgjs5638/chainAnalyzer/config/bigquery-service-account.json"
	client, err := bigquery.NewClient(ctx, pti.projectID, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		return fmt.Errorf("bigquery client 생성 실패: %v", err)
	}
	defer client.Close()

	// 내 프로젝트의 데이터셋에서 가져오는 쿼리
	query := fmt.Sprintf(`
		SELECT 
			`+"`hash`"+`,
			block_timestamp,
			from_address,
			to_address,
			nonce
		FROM `+"`whaletracker-453817.crypto_ethereum.transactions`"+`
		WHERE block_timestamp >= TIMESTAMP('%s')
		  AND block_timestamp < TIMESTAMP('%s')
		  AND from_address IS NOT NULL
		ORDER BY block_timestamp ASC
	`, startDate, endDate)

	q := client.Query(query)
	q.UseStandardSQL = true
	// 쿼리를 내 프로젝트에서 실행하도록 location 설정 
	q.Location = "US"

	// 쿼리 실행
	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("쿼리 실행 실패: %v", err)
	}

	// CSV 파일에 저장
	return pti.saveToCSV(it, batchNum, startDate, endDate)
}

// saveToCSV 결과를 CSV 파일로 저장
func (pti *ParallelTransactionIngester) saveToCSV(it *bigquery.RowIterator, batchNum int, startDate, endDate string) error {
	filename := fmt.Sprintf("%s%s_to_%s_batch_%03d.csv", csvFilePrefix, startDate, endDate, batchNum)
	filepath := filepath.Join(dataDir, filename)

	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("파일 생성 실패: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// CSV 헤더 (RawTransaction 필드에 맞춤)
	header := []string{"block_time", "tx_id", "from", "to", "nonce"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("헤더 작성 실패: %v", err)
	}

	var count int
	for {
		var row BigQueryTxRow
		err := it.Next(&row)

		if err == iterator.Done {
			break
		}

		if err != nil {
			return fmt.Errorf("데이터 읽기 실패: %v", err)
		}

		// RawTransaction에 맞게 변환
		rawTx := pti.convertToRawTransaction(row)

		record := []string{
			rawTx.BlockTime.Format(time.RFC3339),
			rawTx.TxId,
			rawTx.From,
			rawTx.To,
			rawTx.Nonce,
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("레코드 작성 실패: %v", err)
		}

		count++
		if count%50000 == 0 {
			log.Printf("배치 %d: %d개 트랜잭션 처리됨", batchNum, count)
		}
	}

	log.Printf("배치 %d 파일 저장 완료: %s (%d개 트랜잭션)", batchNum, filename, count)
	return nil
}

// convertToRawTransaction BigQuery 로우를 RawTransaction으로 변환
func (pti *ParallelTransactionIngester) convertToRawTransaction(row BigQueryTxRow) domain.RawTransaction {
	// to_address가 NULL인 경우 빈 문자열로 처리 (컨트랙트 생성)
	toAddress := row.ToAddress
	if toAddress == "" {
		toAddress = "" // RawTransaction에서는 빈 문자열로 유지
	}

	return domain.RawTransaction{
		BlockTime: row.BlockTimestamp,
		TxId:      row.Hash,
		From:      row.FromAddress,
		To:        toAddress,
		Nonce:     strconv.FormatInt(row.Nonce, 10),
	}
}

// LoadRawTransactionsFromCSV CSV 파일들을 읽어서 RawTransaction 슬라이스로 반환
func LoadRawTransactionsFromCSV(pattern string) ([]domain.RawTransaction, error) {
	files, err := filepath.Glob(filepath.Join(dataDir, pattern))
	if err != nil {
		return nil, fmt.Errorf("파일 패턴 매칭 실패: %v", err)
	}

	var allTransactions []domain.RawTransaction

	for _, filename := range files {
		log.Printf("CSV 파일 로딩: %s", filename)

		file, err := os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("파일 열기 실패 %s: %v", filename, err)
		}

		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		file.Close()

		if err != nil {
			return nil, fmt.Errorf("CSV 읽기 실패 %s: %v", filename, err)
		}

		// 헤더 스킵하고 데이터 파싱
		for i, record := range records {
			if i == 0 {
				continue // 헤더 스킵
			}

			if len(record) < 5 {
				log.Printf("잘못된 레코드 형식 (파일 %s, 라인 %d): %v", filename, i+1, record)
				continue
			}

			blockTime, err := time.Parse(time.RFC3339, record[0])
			if err != nil {
				log.Printf("시간 파싱 실패 (파일 %s, 라인 %d): %v", filename, i+1, err)
				continue
			}

			rawTx := domain.RawTransaction{
				BlockTime: blockTime,
				TxId:      record[1],
				From:      record[2],
				To:        record[3],
				Nonce:     record[4],
			}

			allTransactions = append(allTransactions, rawTx)
		}

		log.Printf("파일 %s에서 %d개 트랜잭션 로드", filename, len(records)-1)
	}

	log.Printf("총 %d개 트랜잭션 로드 완료", len(allTransactions))
	return allTransactions, nil
}

// GetEstimatedTransactionCount 기간 내 트랜잭션 개수 추정 (사전 확인용)
func (pti *ParallelTransactionIngester) GetEstimatedTransactionCount() (int64, error) {
	ctx := context.Background()
	credentialsFile := "/home/rlaaudgjs5638/chainAnalyzer/config/bigquery-service-account.json"
	client, err := bigquery.NewClient(ctx, pti.projectID, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		return 0, fmt.Errorf("bigquery client 생성 실패: %v", err)
	}
	defer client.Close()

	// 내 프로젝트의 데이터셋에서 카운트 쿼리  
	query := fmt.Sprintf(`
		SELECT COUNT(*) as total_count
		FROM `+"`whaletracker-453817.crypto_ethereum.transactions`"+`
		WHERE block_timestamp >= TIMESTAMP('%s')
		  AND block_timestamp < TIMESTAMP('%s')
		  AND from_address IS NOT NULL
	`, pti.startDate, pti.endDate)

	q := client.Query(query)
	q.UseStandardSQL = true
	// 쿼리를 내 프로젝트에서 실행하도록 location 설정 
	q.Location = "US"

	it, err := q.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("카운트 쿼리 실행 실패: %v", err)
	}

	var row struct {
		TotalCount int64 `bigquery:"total_count"`
	}

	err = it.Next(&row)
	if err != nil {
		return 0, fmt.Errorf("카운트 결과 읽기 실패: %v", err)
	}

	return row.TotalCount, nil
}
