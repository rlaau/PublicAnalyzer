package main

import (
	"context"
	"log"
	"os"
	"time"
)

func main() {
	ctx := context.Background()

	project := os.Getenv("GOOGLE_CLOUD_PROJECT")
	usePrecomp := os.Getenv("USE_PRECOMP") == "1" // 1이면 내 precomp 테이블 사용
	precompDS := os.Getenv("PRECOMP_DATASET")     // 예: "your-project.precomp"
	ckptPath := "./contracts_ckpt.json"
	outPath := "./contracts.jsonl"

	// 드라이런 모드 (환경변수로 토글)
	if os.Getenv("MODE") == "dry" {
		err := RunOnce(ctx, Config{
			ProjectID:          project,
			UsePrecomp:         usePrecomp,
			PrecompDataset:     precompDS,
			OutputFile:         "", // 드라이런: 쓰기 안함
			CheckpointPath:     "", // 드라이런: 저장 안함
			SliceStep:          15 * time.Minute,
			BatchLimit:         2000,
			Delay:              2 * time.Minute,
			MaximumBytesBilled: 10 << 30, // 10GiB
			DryRun:             true,
			MaxSlices:          3,
			MaxRows:            50000,
			AuthMode:           "user", // 브라우저 로그인
		})
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	// 실제 실행 (증분)
	err := RunOnce(ctx, Config{
		ProjectID:          project,
		UsePrecomp:         usePrecomp,
		PrecompDataset:     precompDS,
		OutputFile:         outPath,
		CheckpointPath:     ckptPath,
		SliceStep:          30 * time.Minute,
		BatchLimit:         5000,
		Delay:              2 * time.Minute,
		MaximumBytesBilled: 20 << 30, // 20GiB
		DryRun:             false,
		AuthMode:           "user", // 브라우저 로그인
	})
	if err != nil {
		log.Fatal(err)
	}
}
