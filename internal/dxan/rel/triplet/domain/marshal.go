package domain

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
)

// JSON 표면 구조
type fromScalaJSON struct {
	FromAddress string `json:"fromAddress"`
	LastTxId    string `json:"lastTxId"`
	LastTime    string `json:"lastTime"` // RFC3339(UTC)
	Volume      int32  `json:"volume"`
	//* Option 타입에 대해선 이미 MarshalJson등을 선언해 놨음.
	//* 따로 정의하는 경우, 먼저 그 로직 강제 가능함

	ValueOrNull fp.Option[domain.Value] `json:"valueOrNull"`
}

func (fs FromScala) MarshalJSON() ([]byte, error) {
	j := fromScalaJSON{
		FromAddress: "0x" + hex.EncodeToString(fs.FromAddress[:]),
		LastTxId:    "0x" + hex.EncodeToString(fs.LastTxId[:]),
		LastTime:    time.Time(fs.LastTime).UTC().Format(time.RFC3339Nano),
		Volume:      fs.Volume,
		ValueOrNull: fs.ValueOrNull, // Option은 자체 MarshalJSON 사용 (None→null, Some→값)
	}
	return json.Marshal(j)
}

func (fs *FromScala) UnmarshalJSON(b []byte) error {
	var j fromScalaJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}

	// fromAddress
	addrHex := trim0x(j.FromAddress)
	if len(addrHex) != 40 {
		return fmt.Errorf("fromAddress: want 20 bytes(40 hex), got %d", len(addrHex))
	}
	addrBytes, err := hex.DecodeString(addrHex)
	if err != nil {
		return fmt.Errorf("fromAddress hex decode: %w", err)
	}
	copy(fs.FromAddress[:], addrBytes)

	// lastTxId
	txHex := trim0x(j.LastTxId)
	if len(txHex) != 64 {
		return fmt.Errorf("lastTxId: want 32 bytes(64 hex), got %d", len(txHex))
	}
	txBytes, err := hex.DecodeString(txHex)
	if err != nil {
		return fmt.Errorf("lastTxId hex decode: %w", err)
	}
	copy(fs.LastTxId[:], txBytes)

	// lastTime (RFC3339)
	t, err := time.Parse(time.RFC3339Nano, j.LastTime)
	if err != nil {
		return fmt.Errorf("lastTime parse (RFC3339): %w", err)
	}
	fs.LastTime = chaintimer.ChainTime(t.UTC())

	// volume
	fs.Volume = j.Volume

	// valueOrNull (Option은 자체 UnmarshalJSON으로 처리됨)
	fs.ValueOrNull = j.ValueOrNull
	return nil
}
func trim0x(s string) string {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		return s[2:]
	}
	return s
}

// 따로 구현 안 해도 기본 json.Marshal/Unmarshal이 동작하지만,
// 명시적으로 붙여두면 의도가 분명해짐.
func (fss FromScalas) MarshalJSON() ([]byte, error) {
	type alias FromScalas
	return json.Marshal(alias(fss))
}
func (fss *FromScalas) UnmarshalJSON(b []byte) error {
	type alias FromScalas
	var tmp alias
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	*fss = FromScalas(tmp)
	return nil
}
