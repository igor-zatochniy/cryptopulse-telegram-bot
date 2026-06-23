# === Stage 1: Builder ===
FROM golang:1.25.11-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bot main.go

# === Stage 2: Final Minimal Image ===
FROM alpine:3.21
RUN apk --no-cache add ca-certificates tzdata wget

RUN adduser -D -u 10001 appuser
WORKDIR /home/appuser

COPY --from=builder --chown=appuser:appuser /app/bot .
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:8080/live || exit 1

CMD ["./bot"]