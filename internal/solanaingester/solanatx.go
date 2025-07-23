package solanaingester

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type SolanaContractInfo struct {
	PubKey   string `json:"pubkey"`   // 컨트랙트 주소
	Owner    string `json:"owner"`    // BPF Loader 종류
	Deployer string `json:"deployer"` // 실제 배포자
}

// 쿼리 실행 후 솔라나 컨트렉트 받아오는 코드
// *스트리밍 코드가 아닌, 걍 초기화 코드 정도로 쓸 수 있는듯
// * 해당 쿼리를 스트리밍 사용은 불가
func IngestAllSolanaProgramOnce() {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, "your-project-id")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 모든 솔라나 로더 포함 쿼리
	query := client.Query(`
		WITH program_accounts AS (
			-- 모든 솔라나 로더가 소유한 실행 가능한 프로그램들
			SELECT 
				pubkey,
				owner,
				tx_signature
			FROM ` + "`your-project.dataset.solana_accounts`" + ` 
			WHERE executable = true 
			  AND owner IN (
				'NativeLoader1111111111111111111111111111111',      -- Native programs
				'BPFLoader1111111111111111111111111111111111',      -- BPF Loader v1
				'BPFLoader2111111111111111111111111111111111',      -- BPF Loader v2
				'BPFLoaderUpgradeab1e11111111111111111111111',      -- BPF Loader v3 (Upgradeable)
				'LoaderV411111111111111111111111111111111111'       -- Loader v4
			  )
		),
		program_deployers AS (
			-- 각 프로그램의 생성 트랜잭션에서 배포자 추출
			SELECT 
				pa.pubkey,
				pa.owner,
				-- signer=true인 첫 번째 계정의 pubkey = fee payer = deployer
				(SELECT account.pubkey 
				 FROM UNNEST(tx.accounts) as account 
				 WHERE account.signer = true 
				 ORDER BY account.pubkey
				 LIMIT 1) as deployer
			FROM program_accounts pa
			JOIN ` + "`your-project.dataset.solana_transactions`" + ` tx 
				ON pa.tx_signature = tx.signature
			WHERE tx.status = 'Ok'  -- 성공한 트랜잭션만
		)
		-- 최종 결과 (authority 제외)
		SELECT 
			pubkey,
			owner,
			deployer
		FROM program_deployers
		WHERE deployer IS NOT NULL  -- 배포자가 확인된 것만
		ORDER BY pubkey
	`)

	// 쿼리 실행
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}

	var contracts []SolanaContractInfo
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		contract := SolanaContractInfo{
			PubKey:   getString(row[0]),
			Owner:    getString(row[1]),
			Deployer: getString(row[2]),
		}

		contracts = append(contracts, contract)
	}

	// JSON 파일로 저장
	file, err := os.Create("solana_contracts.json")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(contracts); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("총 %d개의 솔라나 컨트랙트 정보를 저장했습니다.\n", len(contracts))

}

func getString(value bigquery.Value) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

type deployerInfo struct {
	address string
	count   int
}
