# === Етап 1: Збирання ===
FROM golang:1.25.11-alpine@sha256:523c3effe300580ed375e43f43b1c9b091b68e935a7c3a92bfcc4e7ed55b18c2 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bot main.go

# === Етап 2: Фінальний мінімальний образ ===
FROM gcr.io/distroless/static-debian12:nonroot@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639

COPY --from=builder --chown=nonroot:nonroot /app/bot /bot
USER nonroot:nonroot

EXPOSE 8080

# Лише liveness. Readiness-проби оркестратора налаштовуйте на /ready.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/bot", "healthcheck"]

CMD ["/bot"]
