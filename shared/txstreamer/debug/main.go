package main

import (
	"fmt"
	"log"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/txstreamer"
)

func main() {
	root := computation.FindProjectRootPath()
	s := txstreamer.NewTxStreamer(root + "/stream_storage/sorted")
	// s.SetTimeLayout(time.RFC3339Nano) // 필요하면 포맷 변경

	txs, err := s.GetTenThousandsTx("2024-06-02", 123456) // 2024-06-02의 123,456번째부터 최대 10,000건
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("got %d tx", len(txs))
	for i := range 10 {
		fmt.Printf("%d번쨰 tx: %v\n", i, txs[i].String())
	}
}
