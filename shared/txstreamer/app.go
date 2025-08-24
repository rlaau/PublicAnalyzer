package txstreamer

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	mmap "github.com/edsrzf/mmap-go"
	domain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// TxStreamer는 정렬 완료된 mmap 파일(tx_YYYY-MM-DD.mm)을 읽어오는 스트리머이다.
// dir는 "…/stream_storage/sorted"를 가리켜야 한다.
type TxStreamer struct {
	dir        string // ex) computation.FindProjectRootpah()+"/stream_storage/sorted"
	timeLayout string // RawTransaction.BlockTime 포맷 (기본 RFC3339)
}

// NewTxStreamer: base 디렉터리를 명시적으로 전달
func NewTxStreamer(sortedDir string) *TxStreamer {
	return &TxStreamer{
		dir:        sortedDir,
		timeLayout: time.RFC3339,
	}
}

// SetTimeLayout: BlockTime 포맷을 바꾸고 싶을 때 사용 (옵션)
func (s *TxStreamer) SetTimeLayout(layout string) {
	if layout != "" {
		s.timeLayout = layout
	}
}

// GetTenThousandsTx(date, index):
// - date: "YYYY-MM-DD"
// - index: 0-based 시작 인덱스
// 반환: 해당 날짜 파일에서 index부터 최대 10,000건의 []RawTransaction
//
//	(남은 건수가 10,000 미만이면 남은 만큼만 반환)
//
// 주의: 파일이 없으면 os.IsNotExist 에러 반환, 사이즈가 깨지면 에러.
func (s *TxStreamer) GetTenThousandsTx(date string, index int64) ([]domain.RawTransaction, error) {
	if date == "" {
		return nil, fmt.Errorf("date is empty (want YYYY-MM-DD)")
	}
	path := filepath.Join(s.dir, fmt.Sprintf("tx_%s.mm", date))

	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	if st.Size()%int64(RecSize) != 0 {
		return nil, fmt.Errorf("corrupted file %s: size %% recSize != 0 (size=%d, recSize=%d)",
			path, st.Size(), RecSize)
	}

	total := st.Size() / int64(RecSize)
	if index < 0 {
		index = 0
	}
	if index >= total {
		// 범위를 벗어나면 빈 결과
		return []domain.RawTransaction{}, nil
	}

	// 최대 10,000건
	const batch = int64(10_000)
	end := index + batch
	if end > total {
		end = total
	}
	count := end - index

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	mem, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	defer mem.Unmap()

	out := make([]domain.RawTransaction, 0, count)

	// 레코드 단위 슬라이스를 도메인으로 디코드
	for i := int64(0); i < count; i++ {
		base := (index + i) * int64(RecSize)
		seg := mem[base : base+int64(RecSize)]
		tx, err := DecodeRecordToRaw(seg, s.timeLayout)
		if err != nil {
			return nil, fmt.Errorf("decode idx=%d (%s): %w", index+i, path, err)
		}
		out = append(out, tx)
	}

	return out, nil
}
