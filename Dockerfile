# Build stage
FROM golang:1.23-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o streamable ./cmd/streamable

# Runtime stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app

# 실행파일을 /usr/local/bin으로 복사 (마운트에 안 가려짐)
COPY --from=builder /app/streamable /usr/local/bin/streamable

EXPOSE 8080
CMD ["streamable"]

