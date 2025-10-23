# Build stage
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the streamable binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o streamable ./cmd/streamable

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/streamable .

# Expose API port
EXPOSE 8080

# Run the application
CMD ["./streamable"]
