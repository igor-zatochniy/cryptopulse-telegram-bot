# === Етап 1: Збирання ===
FROM golang:1.25.11-alpine@sha256:523c3effe300580ed375e43f43b1c9b091b68e935a7c3a92bfcc4e7ed55b18c2 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bot main.go

# === Етап 2: Фінальний мінімальний образ ===
FROM alpine:3.21@sha256:48b0309ca019d89d40f670aa1bc06e426dc0931948452e8491e3d65087abc07d
RUN apk --no-cache add ca-certificates tzdata

RUN adduser -D -u 10001 appuser
WORKDIR /home/appuser

COPY --from=builder --chown=appuser:appuser /app/bot .
USER appuser

EXPOSE 8080

# Лише liveness. Readiness-проби оркестратора налаштовуйте на /ready.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["./bot", "healthcheck"]

CMD ["./bot"]
