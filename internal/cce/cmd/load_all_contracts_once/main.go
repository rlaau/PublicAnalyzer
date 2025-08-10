package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/cce/infra"
)

func main() {
	ctx := context.Background()

	project := os.Getenv("GOOGLE_CLOUD_PROJECT")
	usePrecomp := os.Getenv("USE_PRECOMP") == "1" // 1이면 내 precomp 테이블 사용
	precompDS := os.Getenv("PRECOMP_DATASET")     // 예: "chain-analyzer-eth-1754795549.precomp"
	badgerPath := "./contractdb"
	ckptPath := "./contracts_ckpt.json"

	// 드라이런 모드 (환경변수로 토글)
	if os.Getenv("MODE") == "dry" {
		err := RunOnce(ctx, Config{
			ProjectID:          project,
			UsePrecomp:         usePrecomp,
			PrecompDataset:     precompDS,
			Repo:               nil, // 드라이런: 쓰기 안함
			CheckpointPath:     "",  // 드라이런: 저장 안함
			SliceStep:          15 * time.Minute,
			BatchLimit:         2000,
			Delay:              2 * time.Minute,
			MaximumBytesBilled: 10 << 30, // 10GiB
			DryRun:             true,
			MaxSlices:          3,
			MaxRows:            50000,
		})
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	// 실제 실행 (증분)
	repo, err := infra.NewRepo(badgerPath)
	if err != nil {
		log.Fatal(err)
	}
	defer repo.Close()

	err = RunOnce(ctx, Config{
		ProjectID:          project,
		UsePrecomp:         usePrecomp,
		PrecompDataset:     precompDS,
		Repo:               repo,
		CheckpointPath:     ckptPath,
		SliceStep:          30 * time.Minute,
		BatchLimit:         5000,
		Delay:              2 * time.Minute,
		MaximumBytesBilled: 20 << 30, // 20GiB
		DryRun:             false,
	})
	if err != nil {
		log.Fatal(err)
	}
}
