package mode

type ProcessingMode int

const (
	TestingModeProcess ProcessingMode = iota
	ProductionModeProcess
)

func (m ProcessingMode) IsTest() bool {
	if m == TestingModeProcess {
		return true
	}
	if m == ProductionModeProcess {
		return false
	}
	panic("mode: ProcessingMode.IsTest() -> 입략값 에러")
}
