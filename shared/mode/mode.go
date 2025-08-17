package mode

type ProcessMode int

const (
	TestingModeProcess ProcessMode = iota
	ProductionModeProcess
)
