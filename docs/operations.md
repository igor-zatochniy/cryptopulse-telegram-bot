# Operations Runbook

Цей документ описує базову експлуатацію CryptoPulse Telegram Bot у production-середовищі.

Мета runbook: швидко зрозуміти, як безпечно деплоїти сервіс, перевіряти його стан, захищати PostgreSQL, робити backup/restore і діяти під час інцидентів.

## Архітектура Production

```text
Telegram Webhook
        |
        v
Koyeb Web Service  --->  Telegram Bot API
        |
        +----------->  Binance Public API
        |
        v
Hetzner PostgreSQL
```

Основні компоненти:

- `Koyeb Web Service` запускає Docker-образ застосунку.
- `Hetzner PostgreSQL` зберігає підписників, налаштування мов, інтервали сповіщень, кеш цін, webhook inbox і cron outbox.
- `cron-job.org` викликає `POST /cron` за розкладом.
- Telegram викликає `POST /webhook`.
- `/ready` перевіряє доступність PostgreSQL.
- `/metrics` віддає Prometheus-метрики за Bearer authentication.

## Production Placeholders

У командах нижче використовуйте власні значення:

```text
<service-domain>             Koyeb domain застосунку
<hetzner_server_ip>          IPv4 адреса сервера з PostgreSQL
<current_koyeb_egress_ip>    поточний outbound IP Koyeb, який бачить PostgreSQL
<database_name>              назва production-бази PostgreSQL
<backup_file>                шлях до backup-файлу PostgreSQL
```

Не комітьте реальні secrets, database URLs, Telegram tokens або cron tokens.

## Deployment Checklist

Перед деплоєм:

- Відкрити pull request і переконатися, що CI green до merge в `main`.
- Перевірити, що `.env.example` не містить реальних секретів.
- Для schema changes зробити backup PostgreSQL до merge.
- Переконатися, що нова migration є backward-compatible для поточної версії сервісу.
- Merge в `main` і дочекатися нового Koyeb deployment.
- Перевірити в startup logs успішне застосування migrations та schema verification.
- Перевірити `/live`, `/ready`, `/metrics`.
- Перевірити, що cron-job.org отримує `200 OK` від `/cron`.

## Deployment Order

Правильний порядок для змін, які зачіпають database schema:

```text
1. Green CI on pull request
2. Backup PostgreSQL
3. Merge backward-compatible migration and code
4. Koyeb starts the new image
5. New instance: migrate -> verify schema -> start HTTP
6. Koyeb readiness succeeds and replaces the old deployment
7. Check /metrics and cron execution logs
```

У поточній Koyeb Git-driven схемі немає окремого release job. Тому migration set вбудований у той самий binary, який запускає сервіс. PostgreSQL advisory lock не дозволяє кільком новим replicas одночасно змінювати schema. HTTP server і `/ready` запускаються тільки після `goose up` та `VerifySchema`.

Цей порядок безпечний лише для expand/backward-compatible migrations. Не видаляйте columns, constraints або значення, які ще використовує попередній deployment. Destructive contract migration виконуйте окремо після повного переходу коду та перевіреного backup.

## Database Migrations

SQL-файли в `migrations/` вбудовуються в application binary. На startup застосунок автоматично:

1. підключається до PostgreSQL;
2. отримує session-level advisory lock для migration process;
3. застосовує всі pending Goose migrations;
4. перевіряє required tables і columns;
5. лише після цього запускає HTTP server.

Для ручної перевірки або аварійного адміністрування з безпечного admin host:

```bash
go install github.com/pressly/goose/v3/cmd/goose@v3.25.0
goose -dir migrations postgres "$DATABASE_URL" status
goose -dir migrations postgres "$DATABASE_URL" up
```

Помилка `database migration failed` означає, що pending migration не застосувалася. Помилка `database schema incompatible` означає, що migration set не створив schema, потрібну поточному binary. В обох випадках новий instance не стане ready; не виконуйте `goose fix` або ручне оновлення `goose_db_version` без аналізу причини.

Якщо production DB раніше вже отримала схему через старий ручний SQL-файл, startup migration зафіксує сумісний стан у `goose_db_version`. Якщо migration зупиниться через дублікати або неконсистентні дані, не форсуйте версію вручну: спочатку виправте дані або відновіть backup.

Якщо міграції потрібно запустити вручну на сервері Hetzner, спочатку склонуйте або оновіть репозиторій із актуальним каталогом `migrations`, потім виконайте ті самі `goose` команди з production `DATABASE_URL`.

Правило для підтримки схеми: вже застосовані migration-файли не редагуються після деплою. Нова зміна схеми додається наступним номером, наприклад `008_some_schema_change.sql`.

Після міграції перевірте ключові таблиці:

```bash
sudo -u postgres psql -d <database_name> -c "\dt"
sudo -u postgres psql -d <database_name> -c "\d subscribers"
sudo -u postgres psql -d <database_name> -c "\d market_prices"
sudo -u postgres psql -d <database_name> -c "\d notification_jobs"
sudo -u postgres psql -d <database_name> -c "\d telegram_updates"
sudo -u postgres psql -d <database_name> -c "\d telegram_replies"
```

## PostgreSQL Firewall

PostgreSQL не має бути відкритим для всього інтернету.

Небезпечний стан:

```text
5432/tcp ALLOW Anywhere
```

Бажаний стан:

```text
5432/tcp дозволений лише для trusted egress IP або private/VPN path
```

Перевірити, хто слухає порт `5432`:

```bash
sudo ss -tulpn | grep ':5432'
```

Перевірити активні PostgreSQL connections:

```bash
sudo -u postgres psql -d <database_name> -c "SELECT client_addr, usename, application_name, state, count(*) FROM pg_stat_activity WHERE datname='<database_name>' GROUP BY client_addr, usename, application_name, state ORDER BY count(*) DESC;"
```

Приклад iptables hardening для поточного Koyeb egress IP:

```bash
sudo iptables -I INPUT 1 -i eth0 -p tcp -s <current_koyeb_egress_ip> --dport 5432 -m comment --comment "ALLOW postgres from Koyeb current egress" -j ACCEPT
sudo iptables -I INPUT 2 -i eth0 -p tcp --dport 5432 -m comment --comment "DROP public postgres" -j DROP
sudo ip6tables -I INPUT 1 -i eth0 -p tcp --dport 5432 -m comment --comment "DROP public postgres ipv6" -j DROP
```

Перевірити правила:

```bash
sudo iptables -L INPUT -n -v --line-numbers | grep 5432
sudo ip6tables -L INPUT -n -v --line-numbers | grep 5432
```

Зберегти правила після перевірки:

```bash
sudo netfilter-persistent save
sudo netfilter-persistent reload
```

Якщо Koyeb змінить outbound IP, `/ready` почне повертати error. У такому випадку потрібно оновити allow-rule для нового egress IP або перейти на managed PostgreSQL/private networking/static egress.

## Backup

Робіть backup перед кожною schema migration і перед ризиковими змінами firewall/networking.

На Hetzner:

```bash
sudo mkdir -p /root/backups
sudo -u postgres pg_dump -Fc -d <database_name> -f /root/backups/cryptopulse_YYYYMMDD_HHMM.dump
```

Перевірити, що backup створився:

```bash
sudo ls -lh /root/backups
```

Рекомендації:

- зберігати кілька останніх backup-файлів;
- періодично копіювати backup за межі сервера;
- тестувати restore на окремій тестовій базі;
- не зберігати backup у Git.

## Restore

Перед restore переконайтеся, що це правильна база і правильний backup.

Приклад restore у наявну базу:

```bash
sudo -u postgres pg_restore --clean --if-exists -d <database_name> /root/backups/<backup_file>
```

Після restore:

```bash
sudo -u postgres psql -d <database_name> -c "SELECT COUNT(*) FROM subscribers;"
sudo -u postgres psql -d <database_name> -c "SELECT COUNT(*) FROM market_prices;"
```

## Health Checks

Liveness:

```bash
curl -fsS https://<service-domain>/live
```

Readiness:

```bash
curl -fsS https://<service-domain>/ready
```

Очікування:

- `/live` повертає `200 OK`, якщо процес живий.
- `/ready` повертає `200 OK`, якщо PostgreSQL доступний.
- Якщо `/live` OK, але `/ready` fail, проблема майже напевно в DB/network/firewall/DATABASE_URL.

## Integration Tests

PostgreSQL integration tests запускаються окремим CI job. У CI вони використовують PostgreSQL service через `INTEGRATION_DATABASE_URL`; локально без цієї змінної вони використовують `testcontainers-go` і потребують Docker. Перед integration tests CI також застосовує `goose` migrations до чистого PostgreSQL:

```bash
go test -tags=integration ./...
```

Вони перевіряють:

- сценарій `setlang` до `/subscribe`;
- `/interval` для неактивного та активного підписника;
- cron claim/release;
- оновлення `last_sent` тільки після успішної Telegram-доставки;
- permanent Telegram error -> unsubscribe;
- transient Telegram error -> retry later без оновлення `last_sent`;
- exhausted transient errors -> тимчасовий cooldown через `delivery_suspended_until`.
- unsubscribe cancellation: активний scheduled job переходить у `canceled` і не надсилається після підтвердження відписки.
- notification outbox retention: `sent` і `canceled` jobs 30 днів, `failed` jobs 90 днів.
- telegram webhook inbox: update зберігається до `200 OK`, worker переводить його в `processed`.
- telegram reply outbox: send/edit відповіді фіксуються до `processed` і повторюються незалежно від команди.
- telegram shard routing: 64 постійні logical shards розподіляються між змінною кількістю workers.
- telegram webhook ordering: відкритий earlier update блокує claim пізнішого update того самого chat, а PostgreSQL advisory lock не дає двом replicas одночасно обробляти один chat.
- language settings: мова читається напряму з PostgreSQL, тому replicas не тримають застаріле локальне значення.
- notification ownership: outbox worker завершує job тільки з актуальним `claim_token`.
- stale workers: фіналізація inbox/outbox job дозволена тільки для поточного `attempts`, тому застарілий worker не перезаписує новий claim.
- telegram inbox retention: `processed` updates 7 днів, `failed` updates 30 днів.
- telegram reply retention: `sent` replies 7 днів, `failed` replies 30 днів.

## Cron

Cron endpoint приймає тільки `POST` і потребує Bearer token:

```bash
curl -X POST \
  -H "Authorization: Bearer $CRON_SECRET" \
  https://<service-domain>/cron
```

Очікувані відповіді:

- `200 OK`: cron batch оброблено або немає due subscribers.
- `401 Unauthorized`: неправильний або відсутній token.
- `409 Conflict`: попередній cron run ще виконується.
- `429 Too Many Requests`: rate limit.
- `500 Internal Server Error`: помилка DB або оновлення cron state.

## Metrics

Metrics endpoint потребує Bearer token:

```bash
curl -H "Authorization: Bearer $METRICS_SECRET" \
  https://<service-domain>/metrics
```

Корисні метрики:

- `cryptopulse_cron_runs_total`
- `cryptopulse_cron_claimed_subscribers_total`
- `cryptopulse_cron_deliveries_total`
- `cryptopulse_webhook_updates_total`
- `cryptopulse_telegram_send_errors_total`
- `cryptopulse_telegram_replies_total`
- `cryptopulse_binance_requests_total`
- `cryptopulse_price_age_seconds`

На що дивитися:

- зростання `telegram_send_errors_total`;
- часті `cron_runs_total{status="conflict"}`;
- `webhook_updates_total{status="persist_error"}`;
- backlog у `telegram_updates` зі статусами `pending` або `processing`;
- backlog у `telegram_replies` зі статусами `pending` або `sending`;
- `binance_requests_total{status!="success"}`;
- `price_age_seconds > 60`, що означає застарілі ринкові дані;
- DB errors у structured logs.

## Incident Playbooks

### `/ready` Fails

1. Перевірити Koyeb logs.
2. Перевірити `DATABASE_URL` у Koyeb environment variables.
3. На Hetzner перевірити PostgreSQL:

```bash
sudo systemctl status postgresql
sudo ss -tulpn | grep ':5432'
```

4. Перевірити firewall rules:

```bash
sudo iptables -L INPUT -n -v --line-numbers | grep 5432
sudo ip6tables -L INPUT -n -v --line-numbers | grep 5432
```

5. Якщо Koyeb egress IP змінився, оновити allow-rule.

### Cron Does Not Send Messages

1. Перевірити cron-job.org history.
2. Перевірити method: має бути `POST`.
3. Перевірити header:

```text
Authorization: Bearer <CRON_SECRET>
```

4. Перевірити `/metrics`:

```text
cryptopulse_cron_runs_total
cryptopulse_cron_deliveries_total
cryptopulse_telegram_send_errors_total
```

5. Перевірити subscribers:

```bash
sudo -u postgres psql -d <database_name> -c "SELECT chat_id, interval_minutes, last_sent, is_subscribed, cron_claimed_until, delivery_suspended_until FROM subscribers ORDER BY last_sent ASC LIMIT 20;"
```

6. Перевірити outbox backlog і retention:

```bash
sudo -u postgres psql -d <database_name> -c "SELECT status, COUNT(*) FROM notification_jobs GROUP BY status ORDER BY status;"
sudo -u postgres psql -d <database_name> -c "SELECT chat_id, COUNT(*) FROM notification_jobs WHERE status IN ('pending', 'sending') GROUP BY chat_id HAVING COUNT(*) > 1;"
```

7. Перевірити webhook inbox і reply outbox:

```bash
sudo -u postgres psql -d <database_name> -c "SELECT status, COUNT(*) FROM telegram_updates GROUP BY status ORDER BY status;"
sudo -u postgres psql -d <database_name> -c "SELECT update_id, chat_id, status, attempts, claimed_until, next_attempt_at, last_error FROM telegram_updates WHERE status IN ('pending', 'processing', 'failed') ORDER BY update_id DESC LIMIT 20;"
sudo -u postgres psql -d <database_name> -c "SELECT chat_id, COUNT(*) FROM telegram_updates WHERE status IN ('pending', 'processing') GROUP BY chat_id HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC LIMIT 20;"
sudo -u postgres psql -d <database_name> -c "SELECT status, COUNT(*) FROM telegram_replies GROUP BY status ORDER BY status;"
sudo -u postgres psql -d <database_name> -c "SELECT id, source_update_id, chat_id, operation, status, attempts, next_attempt_at, last_error FROM telegram_replies WHERE status IN ('pending', 'sending', 'failed') ORDER BY id DESC LIMIT 20;"
```

### Telegram Webhook Does Not Work

1. Перевірити Koyeb logs на `unauthorized webhook`.
2. Перевірити `WEBHOOK_SECRET_TOKEN`.
3. Перевірити, що Telegram webhook URL вказує на:

```text
https://<service-domain>/webhook
```

4. Перевірити, що endpoint приймає тільки `POST`.
5. Перевірити durable inbox і reply outbox:

```bash
sudo -u postgres psql -d <database_name> -c "SELECT status, COUNT(*) FROM telegram_updates GROUP BY status ORDER BY status;"
sudo -u postgres psql -d <database_name> -c "SELECT status, COUNT(*) FROM telegram_replies GROUP BY status ORDER BY status;"
```

### Binance Prices Are Stale

1. Перевірити logs на `binance`.
2. Перевірити metrics:

```text
cryptopulse_binance_requests_total
```

3. Перевірити price cache у DB:

```bash
sudo -u postgres psql -d <database_name> -c "SELECT symbol, price, updated_at FROM market_prices ORDER BY symbol;"
```

### Suspected Public PostgreSQL Exposure

1. Перевірити з зовнішньої машини, чи доступний порт:

```bash
nc -vz <hetzner_server_ip> 5432
```

2. Якщо порт відкритий для всіх, негайно додати DROP rule для public traffic.
3. Перевірити PostgreSQL logs на brute-force attempts.
4. Змінити password для application user, якщо є підозра на витік.
5. Оновити `DATABASE_URL` у Koyeb і redeploy.

## Secret Rotation

Коли ротувати secrets:

- після підозри на витік;
- після випадкового показу token у logs/screenshots;
- після передачі доступу іншій людині;
- періодично для production hygiene.

Що ротувати:

- `TELEGRAM_APITOKEN`
- `WEBHOOK_SECRET_TOKEN`
- `CRON_SECRET`
- `METRICS_SECRET`
- PostgreSQL password у `DATABASE_URL`

Після rotation:

1. Оновити Koyeb environment variables.
2. Redeploy service.
3. Перевірити `/ready`.
4. Перевірити `/cron`.
5. Перевірити Telegram webhook.

## Rollback

Якщо новий deployment зламав застосунок:

1. У Koyeb відкотитися на попередній deployment/image.
2. Перевірити `/live` і `/ready`.
3. Перевірити logs.
4. Якщо була застосована destructive migration, відновити DB з backup.

Поточні migrations здебільшого additive/hardening, але backup перед їх запуском все одно обов'язковий.

## Production Readiness Notes

Поточний проєкт має хороший production baseline:

- Docker non-root runtime;
- pinned image digests;
- DB migration;
- authenticated cron/webhook/metrics;
- durable webhook inbox і cron outbox;
- per-chat Telegram update ordering через PostgreSQL claims/advisory locks;
- JSON structured logs;
- Prometheus metrics;
- graceful shutdown;
- CI checks;
- PostgreSQL integration tests;
- secret scanning.

Наступні покращення для більшого масштабу:

- static egress/private networking для Koyeb-to-PostgreSQL;
- automated backup schedule.
