# === Етап 1: Збирання ===
FROM golang:1.25.13-alpine@sha256:844b27705f54e73773e0f9bc3c780633b9d7f4b4831bf35cdad02a81a4c80bd0 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bot ./cmd/cryptopulse

# === Етап 2: Фінальний мінімальний образ ===
FROM gcr.io/distroless/static-debian12:nonroot@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639

COPY --from=builder --chown=nonroot:nonroot /app/bot /bot
USER nonroot:nonroot

EXPOSE 8080

# Лише liveness. Readiness-проби оркестратора налаштовуйте на /ready.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/bot", "healthcheck"]

CMD ["/bot"]
