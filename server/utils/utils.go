package utils

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// ParseIntParam URL 파라미터에서 정수값 파싱
func ParseIntParam(r *http.Request, paramName string, defaultValue int) int {
	if value := r.URL.Query().Get(paramName); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

// WriteErrorResponse 에러 응답 생성
func WriteErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := map[string]string{
		"error":   message,
		"service": "triplet-analyzer",
	}

	json.NewEncoder(w).Encode(response)
}

// WriteJSONResponse JSON 응답 생성
func WriteJSONResponse(w http.ResponseWriter, data interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(data)
}
