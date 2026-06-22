# === Stage 1: Builder ===
FROM golang:1.25.4-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bot main.go

# === Stage 2: Final Minimal Image ===
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

RUN adduser -D -u 10001 appuser
WORKDIR /home/appuser

COPY --from=builder /app/bot .
RUN chown -R appuser:appuser /home/appuser

USER appuser

CMD ["./bot"]