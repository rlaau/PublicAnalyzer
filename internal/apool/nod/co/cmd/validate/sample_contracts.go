package main

import (
	"fmt"
	"log"
	"math/rand"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

const (
	SAMPLE_SIZE = 1000
)

type ContractSample struct {
	Address domain.Address
	Creator domain.Address
}

func main() {
	log.Printf("Opening ContractDB to sample %d contracts...", SAMPLE_SIZE)

	// 프로덕션 모드로 ContractDB 열기
	db, err := infra.NewContractDB(mode.ProductionModeProcess, "")
	if err != nil {
		log.Fatalf("Failed to open ContractDB: %v", err)
	}
	defer db.Close()

	// 전체 데이터 개수 확인
	totalCount := 68654317 //걍 세보니까 이렇게 나옴
	if err != nil {
		log.Fatalf("Failed to get total count: %v", err)
	}

	log.Printf("Total contracts in DB: %d", totalCount)

	if totalCount == 0 {
		log.Fatalf("No contracts found in database")
	}

	// 샘플링할 개수 조정
	sampleSize := SAMPLE_SIZE
	if totalCount < SAMPLE_SIZE {
		sampleSize = totalCount
		log.Printf("Adjusting sample size to %d (total available)", sampleSize)
	}

	// 랜덤 샘플링
	samples, err := getRandomSamples(db, sampleSize, totalCount)
	if err != nil {
		log.Fatalf("Failed to get random samples: %v", err)
	}

	log.Printf("Successfully sampled %d contracts", len(samples))
	// 샘플 결과 출력
	printSamples(samples)

	// 통계 출력
	printStatistics(samples)
}

func getRandomSamples(db *infra.ContractDB, sampleSize, totalCount int) ([]ContractSample, error) {
	samples := make([]ContractSample, 0, sampleSize)

	// 샘플링 비율 계산 (전체에서 몇 개를 골라야 할지)
	samplingRate := float64(1)

	// DB에 직접 접근해서 iterator 사용하여 랜덤 샘플링
	err := db.IterateContracts(func(position int, contract domain.Contract) bool {
		// 랜덤하게 선택 (샘플링 확률 기반)
		if rand.Float64() < samplingRate {
			samples = append(samples, ContractSample{
				Address: contract.Address,
				Creator: contract.CreatorOrZero,
			})
		}

		// 목표 샘플 수에 도달하면 중단
		return len(samples) < sampleSize
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate contracts: %w", err)
	}

	log.Printf("Retrieved %d sample contracts from database", len(samples))
	return samples, nil
}

func printSamples(samples []ContractSample) {
	log.Printf("=== Random Contract Samples ===")

	// 처음 20개와 마지막 10개만 출력
	printCount := 20
	if len(samples) < printCount {
		printCount = len(samples)
	}

	fmt.Printf("\nFirst %d samples:\n", printCount)
	for i := 0; i < printCount; i++ {
		sample := samples[i]
		creatorStr := "nil"
		if !domain.IsNullAddress(sample.Creator) {
			creatorStr = sample.Creator.String()
		}

		fmt.Printf("[%03d] Contract: %s\n      Creator:  %s\n",
			i+1, sample.Address.String(), creatorStr)
	}

	if len(samples) > 30 {
		fmt.Printf("\n... (skipping middle samples) ...\n")

		fmt.Printf("\nLast 10 samples:\n")
		for i := len(samples) - 10; i < len(samples); i++ {
			sample := samples[i]
			creatorStr := "nil"
			if !domain.IsNullAddress(sample.Creator) {
				creatorStr = sample.Creator.String()
			}

			fmt.Printf("[%03d] Contract: %s\n      Creator:  %s\n",
				i+1, sample.Address.String(), creatorStr)
		}
	}
}

func printStatistics(samples []ContractSample) {
	log.Printf("=== Statistics ===")

	// Creator가 설정된 개수 계산
	withCreator := 0
	withoutCreator := 0

	for _, sample := range samples {
		if domain.IsNullAddress(sample.Creator) {
			withoutCreator++
		} else {
			withCreator++
		}
	}

	fmt.Printf("Total samples: %d\n", len(samples))
	fmt.Printf("With creator set: %d (%.2f%%)\n",
		withCreator, float64(withCreator)/float64(len(samples))*100)
	fmt.Printf("Without creator (zero address): %d (%.2f%%)\n",
		withoutCreator, float64(withoutCreator)/float64(len(samples))*100)

	// 중복 creator 찾기 (간단한 통계)
	creatorMap := make(map[domain.Address]int)
	for _, sample := range samples {
		if !domain.IsNullAddress(sample.Creator) {
			creatorMap[sample.Creator]++
		}
	}

	// 가장 많이 나타나는 creator들
	type creatorCount struct {
		addr  domain.Address
		count int
	}

	var topCreators []creatorCount
	for addr, count := range creatorMap {
		if count > 1 { // 2개 이상 생성한 경우만
			topCreators = append(topCreators, creatorCount{addr, count})
		}
	}

	if len(topCreators) > 0 {
		fmt.Printf("\nCreators with multiple contracts in sample:\n")
		for i, creator := range topCreators {
			if i >= 5 { // 상위 5개만 출력
				break
			}
			fmt.Printf("  %s: %d contracts\n", creator.addr.String(), creator.count)
		}
	} else {
		fmt.Printf("\nNo creators with multiple contracts found in sample\n")
	}
}
