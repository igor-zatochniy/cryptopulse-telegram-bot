# CryptoPulse Telegram Bot

[![CI](https://github.com/igor-zatochniy/cryptopulse-telegram-bot/actions/workflows/ci.yml/badge.svg)](https://github.com/igor-zatochniy/cryptopulse-telegram-bot/actions/workflows/ci.yml)
[![Telegram Bot](https://img.shields.io/badge/Telegram-Live%20Bot-2CA5E0?logo=telegram)](https://t.me/btc_eth_usdt_bot)

Production-орієнтований Telegram-сервіс на Go. Він відстежує ціни криптовалют, зберігає налаштування підписників у PostgreSQL, надсилає заплановані Telegram-сповіщення, має health/readiness endpoints і постачається як посилений Docker-образ.

Проєкт навмисно компактний, але побудований як реальний сервіс: міграції бази даних, graceful shutdown, CI-перевірки, Prometheus-метрики, rate limits, Docker hardening і явна обробка операційних помилок.

## Спробувати бота

Відкрийте CryptoPulse у Telegram:

[**Запустити @btc_eth_usdt_bot**](https://t.me/btc_eth_usdt_bot)

Після відкриття натисніть **Start** або надішліть команду:

```text
/start
```

## Demo

![CryptoPulse: запуск бота, актуальні ціни та вибір інтервалу](docs/images/bot-demo-start.png)

![CryptoPulse: планове оновлення, вибір мови та відписка](docs/images/bot-demo-alerts.png)

## Основні команди

| Команда | Призначення |
| --- | --- |
| `/start` | Запустити бота та переглянути вступне повідомлення |
| `/subscribe` | Увімкнути регулярні сповіщення |
| `/unsubscribe` | Вимкнути регулярні сповіщення |
| `/price` | Отримати актуальні ціни криптовалют |
| `/interval` | Змінити інтервал сповіщень |
| `/language` | Вибрати мову повідомлень |

## Deployment

- **Status:** Live
- **Platform:** Koyeb
- **Bot:** [@btc_eth_usdt_bot](https://t.me/btc_eth_usdt_bot)
- **Runtime:** Distroless Docker image, non-root
- **Health checks:** `/live` and `/ready`

## Можливості

- Telegram-команди для підписки, вибору мови, оновлення цін і керування інтервалом сповіщень.
- Заплановані сповіщення про ціни криптовалют через автентифікований endpoint `/cron`.
- Кеш актуальних цін на основі Binance ticker requests із збереженням у PostgreSQL.
- Стан підписників у PostgreSQL зі schema migration, defaults, constraints та indexes.
- Повідомлення бота українською, англійською та російською мовами.
- JSON structured logs через `log/slog`.
- Prometheus-метрики за Bearer authentication.
- Liveness і readiness endpoints для production orchestration.
- Graceful shutdown для HTTP producers і worker pools.
- Посилений Docker runtime на базі distroless і non-root execution.

## Архітектура

```mermaid
flowchart LR
    Telegram["Telegram Webhook"] -->|POST /webhook| App["Go Bot Service"]
    Cron["cron-job.org / Scheduler"] -->|POST /cron + Bearer token| App
    App -->|Ticker requests| Binance["Binance API"]
    App -->|SQL queries| Postgres["PostgreSQL"]
    App -->|Send messages| TelegramAPI["Telegram Bot API"]
    App -->|GET /metrics + Bearer token| Metrics["Prometheus Metrics"]
    App -->|GET /live, /ready| Health["Health Checks"]
```

## Технологічний стек

- Go 1.25.12
- PostgreSQL
- Telegram Bot API
- Binance public ticker API
- Prometheus client metrics
- Docker multi-stage build
- Distroless runtime image
- GitHub Actions CI
- Koyeb deployment

## Структура репозиторію

```text
.
├── .github/workflows/ci.yml      # CI: tests, vet, race, lint, govulncheck, Docker build, gitleaks
├── docs/operations.md            # Production runbook для деплою, DB, firewall і incident response
├── migrations/001_init_schema.sql # PostgreSQL-схема та індекси
├── Dockerfile                    # Multi-stage production image
├── main.go                       # Точка входу застосунку та сервісна логіка
├── main_test.go                  # Regression tests для middleware/auth behavior
├── .golangci.yml                 # Конфігурація golangci-lint v2
├── .env.example                  # Безпечний шаблон environment variables
├── LICENSE                       # MIT license
├── .dockerignore
├── .gitignore
├── go.mod
└── go.sum
```

## Runtime Endpoints

| Endpoint | Метод | Auth | Призначення |
| --- | --- | --- | --- |
| `/live` | `GET` | none | Liveness check. Не звертається до зовнішніх залежностей. |
| `/ready` | `GET` | none | Readiness check. Перевіряє підключення до PostgreSQL. |
| `/webhook` | `POST` | `X-Telegram-Bot-Api-Secret-Token` | Endpoint для Telegram updates. |
| `/cron` | `POST` | `Authorization: Bearer <CRON_SECRET>` | Забирає due subscribers і надсилає заплановані сповіщення. |
| `/metrics` | `GET` | `Authorization: Bearer <CRON_SECRET>` | Prometheus metrics. |

## Змінні Середовища

Скопіюйте приклад і заповніть реальні значення:

```bash
cp .env.example .env
```

Обов'язкові змінні:

| Variable | Обов'язкова | Опис |
| --- | --- | --- |
| `DATABASE_URL` | yes | PostgreSQL connection string. У production використовуйте `sslmode=require`. |
| `TELEGRAM_APITOKEN` | yes | Telegram bot token від BotFather. |
| `WEBHOOK_SECRET_TOKEN` | yes | Secret, який очікується від Telegram webhook requests. |
| `CRON_SECRET` | yes | Bearer secret для `/cron` і `/metrics`. |
| `PORT` | no | HTTP port. За замовчуванням `8080`. |

Ніколи не комітьте реальні `.env` файли або production secrets.

## Міграція бази даних

Застосуйте схему перед деплоєм нової версії сервісу:

```bash
psql "$DATABASE_URL" -f migrations/001_init_schema.sql
```

Міграція створює та посилює:

- `subscribers`
- `market_prices`
- unique indexes для `chat_id` і `symbol`
- defaults і `NOT NULL` constraints
- перевірки interval і language
- cron query indexes
- `cron_claimed_until` для коротких database claims
- `delivery_suspended_until` для cooldown після exhausted transient delivery failures
- retention indexes для очищення старих `notification_jobs`

## Локальна розробка

Встановіть Go 1.25.12 або дозвольте Go toolchain directive завантажити потрібну версію автоматично.

```bash
go mod download
go test ./...
go run .
```

Під час локальної розробки застосунок автоматично завантажує `.env`.

## Docker

Зберіть production image:

```bash
docker build -t cryptopulse-telegram-bot .
```

Запустіть його:

```bash
docker run --rm -p 8080:8080 --env-file .env cryptopulse-telegram-bot
```

Фінальний образ використовує:

- pinned base image digests
- static Go binary
- distroless runtime
- `nonroot:nonroot`
- Docker healthcheck через `/live`

## CI/CD

GitHub Actions запускається на кожен push і pull request:

- `go test ./...`
- `go vet ./...`
- `go test -race ./...`
- `go test -tags=integration ./...`
- `govulncheck`
- `golangci-lint v2.12.0`
- Docker build
- gitleaks secret scan

Поточний CI status показаний badge у верхній частині README.

Integration tests використовують `testcontainers-go`, піднімають PostgreSQL container і потребують доступного Docker daemon.

## Операції

Докладний production runbook: [docs/operations.md](docs/operations.md).

Readiness check:

```bash
curl -fsS https://<service-domain>/ready
```

Приклад cron trigger:

```bash
curl -X POST \
  -H "Authorization: Bearer $CRON_SECRET" \
  https://<service-domain>/cron
```

Приклад metrics request:

```bash
curl -H "Authorization: Bearer $CRON_SECRET" \
  https://<service-domain>/metrics
```

Очікувана поведінка в production:

- `/live` залишається lightweight для liveness checks.
- `/ready` повертає помилку, якщо PostgreSQL недоступний.
- `/cron` відхиляє unauthorized calls і overlapping runs через PostgreSQL advisory lock.
- Permanent Telegram delivery errors відписують недоступних користувачів.
- Transient Telegram errors не оновлюють `last_sent`, щоб дозволити наступну спробу.

## Нотатки З Безпеки

- Secrets читаються тільки з environment variables.
- Telegram webhook requests потребують `WEBHOOK_SECRET_TOKEN`.
- Cron і metrics endpoints потребують Bearer authentication.
- Тіло webhook request має обмеження за розміром.
- HTTP methods явно перевіряються.
- Cron має глобальний rate limit, а webhook обмежується окремо для кожного remote client.
- PostgreSQL не має бути відкритим у public internet. У production обмежуйте `5432/tcp` trusted egress IPs, private networking або VPN.
- Docker runtime не запускається від root.

## Нотатки З Надійності

- Вибір cron subscribers використовує короткі database claims із `FOR UPDATE SKIP LOCKED`.
- Cron jobs створюються як durable rows у PostgreSQL outbox.
- Telegram sends виконуються поза database transactions.
- Успішні sends оновлюють `last_sent`; невдалі sends не оновлюють.
- Transient retry очищає subscriber claim, але pending outbox job не дозволяє створити duplicate notification.
- Після вичерпання transient retry attempts підписник тимчасово призупиняється через `delivery_suspended_until`.
- Outbox retention видаляє `sent` jobs після 30 днів, а `failed` jobs після 90 днів; `pending` і `sending` jobs не видаляються.
- HTTP producers відстежуються через `WaitGroup` під час shutdown.
- Після зупинки producers worker pools дочитують закриті канали, щоб не губити вже прийняті Telegram updates; cron jobs залишаються durable в PostgreSQL outbox.
- Context cancellation використовується як forced fallback, якщо producers не вдалося зупинити вчасно.

## Ключові Інженерні Рішення

У проєкті реалізовано:

- production-oriented Go service design у компактному коді
- PostgreSQL schema hardening і migration discipline
- container hardening із distroless і non-root runtime
- CI pipeline з tests, race detector, linting, vulnerability scanning, Docker build і secret scanning
- PostgreSQL integration tests через testcontainers-go
- реальну operational security роботу, включно із protected metrics, authenticated cron і restricted database exposure

## Ліцензія

Проєкт поширюється за ліцензією MIT. Дивіться [LICENSE](LICENSE).
