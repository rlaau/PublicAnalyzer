package rsv

import (
	"strings"
	"time"
)

// MeasureFunction은 함수 실행 시간을 측정하는 헬퍼 함수입니다.
// defer MeasureFunction(meter, "functionName")() 형태로 사용합니다.
func MeasureFunction(meter *FunctionReservoirMeter, functionName string) func() {
	start := time.Now()
	return func() {
		duration := time.Since(start)
		meter.Record(functionName, duration)
	}
}

// MeasureFunctionWithResult는 함수 실행 시간을 측정하고 결과도 반환하는 헬퍼 함수입니다.
func MeasureFunctionWithResult[T any](meter *FunctionReservoirMeter, functionName string, fn func() T) T {
	start := time.Now()
	result := fn()
	duration := time.Since(start)
	meter.Record(functionName, duration)
	return result
}

// MeasureFunctionWithError는 함수 실행 시간을 측정하고 에러도 처리하는 헬퍼 함수입니다.
func MeasureFunctionWithError[T any](meter *FunctionReservoirMeter, functionName string, fn func() (T, error)) (T, error) {
	start := time.Now()
	result, err := fn()
	duration := time.Since(start)
	
	// 에러가 있는 경우 함수명에 ".error" 추가
	measureName := functionName
	if err != nil {
		measureName = functionName + ".error"
	}
	
	meter.Record(measureName, duration)
	return result, err
}

// GetPackageFunctionName은 패키지.함수명 형태로 정규화합니다.
func GetPackageFunctionName(packageName, functionName string) string {
	if packageName == "" {
		return functionName
	}
	return packageName + "." + functionName
}

// NormalizeMethodName은 메서드명을 정규화합니다.
func NormalizeMethodName(receiverType, methodName string) string {
	if receiverType == "" {
		return methodName
	}
	// 포인터 타입 표시 제거
	receiverType = strings.TrimPrefix(receiverType, "*")
	return receiverType + "." + methodName
}