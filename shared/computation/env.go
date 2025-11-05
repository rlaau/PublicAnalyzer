package computation

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
)

func GetENV() string {
	return filepath.Join(GoModWD(), ".env")
}

type ModeKey string

const PROCESS_MODE_KEY ModeKey = "PROCESS_MODE"

type ProcessMode string

const (
	ProductionMode ProcessMode = "PRODUCTION_MODE"
	TestingMode    ProcessMode = "TESTING_MODE"
)

func SetModeENV(mode ProcessMode) error {
	return os.Setenv(string(PROCESS_MODE_KEY), string(mode))
}

type NameKey string

const PROCESS_NAME_KEY NameKey = "PROCESS_NAME"

type ProcessName string

// 허용 규칙:
//   - 시작: 소문자 또는 숫자
//   - 나머지: 소문자, 숫자, '-', '_', '.'
//   - 길이: 1~128 chars
var processNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

func ParseProcessName(name string) (ProcessName, error) {
	if !processNameRe.MatchString(name) {
		return "", errors.New("올바르지 않은 프로세스 이름입니다. (허용: 소문자/숫자/./_/-, 길이<=128)")
	}
	return ProcessName(name), nil
}

func MustParseProcessName(name string) ProcessName {
	n, err := ParseProcessName(name)
	if err != nil {
		panic(err)
	}
	return n
}

func SetProcessNameENV(name ProcessName) error {
	return os.Setenv(string(PROCESS_NAME_KEY), string(name))
}
