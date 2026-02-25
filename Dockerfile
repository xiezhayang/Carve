# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# 依赖先复制，利用层缓存
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /carve .

# Run stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -g "" carve

WORKDIR /app

# 默认 CSV 写入目录（可通过 CARVE_CSV_DIR 覆盖）
ENV CARVE_CSV_DIR=/app/data
RUN mkdir -p /app/data && chown -R carve:carve /app

COPY --from=builder /carve /app/carve

USER carve

EXPOSE 8080

# K8s 一般用 PORT 或保持默认 8080
CMD ["/app/carve"]