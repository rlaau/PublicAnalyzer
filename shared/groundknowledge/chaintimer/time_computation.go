package chaintimer

import (
	"fmt"
	"time"
)

// NewChainTime은 새로운 ChainTime을 생성합니다
// 미국 기반 시간으로 고정임
func NewChainTime(year int, month time.Month, day, hour, min, sec, nsec int) ChainTime {
	return ChainTime(time.Date(year, month, day, hour, min, sec, nsec, time.UTC))
}

// 각가지 체인 값 받아서 시간 리턴
func ChainDate(year int, month ChainMonth, day, hour, min, sec, nsec int, loc *time.Location) ChainTime {
	return ChainTime(time.Date(year, time.Month(month), day, hour, min, sec, nsec, loc))
}

// Unix는 Unix시간을 ChainTime타입으로 변호나
func Unix(i int64, z int64) ChainTime {
	return ChainTime(time.Unix(i, z))
}

// Parse는 문자열을 ChainTime으로 파싱합니다
func Parse(layout, value string) (ChainTime, error) {
	t, err := time.Parse(layout, value)
	if err != nil {
		return ChainTime{}, err
	}
	return ChainTime(t), nil
}
func ParseEthTime(v string) ChainTime {
	ct, err := Parse(EhtTimeFormat, v)
	if err != nil {
		panic("이더리움 시간 파싱 실패. RFC3339형식이 아니었는듯")
	}
	return ct
}

// ParseInLocation은 특정 location에서 문자열을 ChainTime으로 파싱합니다
func ParseInLocation(layout, value string, loc *time.Location) (ChainTime, error) {
	t, err := time.ParseInLocation(layout, value, loc)
	if err != nil {
		return ChainTime{}, err
	}
	return ChainTime(t), nil
}

// 자주 사용되는 형식들을 위한 편의 함수들
func ParseDate(value string) (ChainTime, error) {
	return Parse("2006-01-02", value)
}

func ParseDateTime(value string) (ChainTime, error) {
	return Parse("2006-01-02 15:04:05", value)
}

func ParseRFC3339(value string) (ChainTime, error) {
	return Parse(time.RFC3339, value)
}

// 자동 형식 감지 (여러 형식 시도)
func ParseChainTimeAuto(value string) (ChainTime, error) {
	layouts := []string{
		time.RFC3339,          // "2006-01-02T15:04:05Z07:00"
		time.RFC3339Nano,      // "2006-01-02T15:04:05.999999999Z07:00"
		"2006-01-02 15:04:05", // "2006-01-02 15:04:05"
		"2006-01-02",          // "2006-01-02"
		"15:04:05",            // "15:04:05"
		time.RFC822,           // "02 Jan 06 15:04 MST"
		time.RFC822Z,          // "02 Jan 06 15:04 -0700"
		time.Kitchen,          // "3:04PM"
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return ChainTime(t), nil
		}
	}

	return ChainTime{}, fmt.Errorf("unable to parse time: %s", value)
}

// 4. ChainTime 메서드로 추가 (권장)
func (ct *ChainTime) UnmarshalText(data []byte) error {
	parsed, err := ParseChainTimeAuto(string(data))
	if err != nil {
		return err
	}
	*ct = parsed
	return nil
}

func (ct ChainTime) MarshalText() ([]byte, error) {
	return []byte(time.Time(ct).Format(time.RFC3339)), nil
}

// 5. JSON 직렬화/역직렬화 지원
func (ct *ChainTime) UnmarshalJSON(data []byte) error {
	// JSON에서 따옴표 제거
	s := string(data)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}

	parsed, err := ParseChainTimeAuto(s)
	if err != nil {
		return err
	}
	*ct = parsed
	return nil
}

func (ct ChainTime) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(ct).Format(time.RFC3339) + `"`), nil
}

func MustParseChainTimeAuto(value string) ChainTime {
	ct, err := ParseChainTimeAuto(value)
	if err != nil {
		panic(err)
	}
	return ct
}
