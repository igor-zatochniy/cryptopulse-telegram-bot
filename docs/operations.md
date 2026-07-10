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
- `Hetzner PostgreSQL` зберігає підписників, налаштування мов, інтервали сповіщень і кеш цін.
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

- Переконатися, що CI green на GitHub.
- Перевірити, що `.env.example` не містить реальних секретів.
- Зробити backup PostgreSQL.
- Застосувати SQL migration до production DB.
- Задеплоїти новий Docker image у Koyeb.
- Перевірити `/live`, `/ready`, `/metrics`.
- Перевірити, що cron-job.org отримує `200 OK` від `/cron`.

## Deployment Order

Правильний порядок для змін, які зачіпають database schema:

```text
1. Backup PostgreSQL
2. Apply migrations/001_init_schema.sql
3. Deploy new Koyeb image
4. Check /ready
5. Check /metrics
6. Check cron execution logs
```

Міграцію потрібно застосувати до деплою нового коду, якщо код залежить від нових колонок, індексів або constraints.

## Database Migration

Локально або з безпечного admin host:

```bash
psql "$DATABASE_URL" -f migrations/001_init_schema.sql
```

На сервері Hetzner можна виконати від імені PostgreSQL admin:

```bash
sudo -u postgres psql -d <database_name> -f /path/to/migrations/001_init_schema.sql
```

Після міграції перевірте ключові таблиці:

```bash
sudo -u postgres psql -d <database_name> -c "\dt"
sudo -u postgres psql -d <database_name> -c "\d subscribers"
sudo -u postgres psql -d <database_name> -c "\d market_prices"
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

PostgreSQL integration tests запускаються окремим CI job і потребують Docker:

```bash
go test -tags=integration ./...
```

Вони перевіряють:

- сценарій `setlang` до `/subscribe`;
- `/interval` для неактивного та активного підписника;
- cron claim/release;
- оновлення `last_sent` тільки після успішної Telegram-доставки;
- permanent Telegram error -> unsubscribe;
- transient Telegram error -> retry later без оновлення `last_sent`.

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
curl -H "Authorization: Bearer $CRON_SECRET" \
  https://<service-domain>/metrics
```

Корисні метрики:

- `cryptopulse_cron_runs_total`
- `cryptopulse_cron_claimed_subscribers_total`
- `cryptopulse_cron_deliveries_total`
- `cryptopulse_webhook_updates_total`
- `cryptopulse_telegram_send_errors_total`
- `cryptopulse_binance_requests_total`

На що дивитися:

- зростання `telegram_send_errors_total`;
- часті `cron_runs_total{status="conflict"}`;
- `webhook_updates_total{status="dropped_queue_full"}`;
- `binance_requests_total{status!="success"}`;
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
sudo -u postgres psql -d <database_name> -c "SELECT chat_id, interval_minutes, last_sent, is_subscribed, cron_claimed_until FROM subscribers ORDER BY last_sent ASC LIMIT 20;"
```

### Telegram Webhook Does Not Work

1. Перевірити Koyeb logs на `unauthorized webhook`.
2. Перевірити `WEBHOOK_SECRET_TOKEN`.
3. Перевірити, що Telegram webhook URL вказує на:

```text
https://<service-domain>/webhook
```

4. Перевірити, що endpoint приймає тільки `POST`.

### Binance Prices Are Stale

1. Перевірити logs на `binance`.
2. Перевірити metrics:

```text
cryptopulse_binance_requests_total
```

3. Перевірити DB cache:

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

Поточна migration `001_init_schema.sql` здебільшого additive/hardening, але backup перед її запуском все одно обов'язковий.

## Production Readiness Notes

Поточний проєкт має хороший production baseline:

- Docker non-root runtime;
- pinned image digests;
- DB migration;
- authenticated cron/webhook/metrics;
- JSON structured logs;
- Prometheus metrics;
- graceful shutdown;
- CI checks;
- PostgreSQL integration tests;
- secret scanning.

Наступні покращення для більшого масштабу:

- persistent outbox для Telegram delivery;
- integration tests із PostgreSQL;
- окремий `METRICS_SECRET`;
- static egress/private networking для Koyeb-to-PostgreSQL;
- automated backup schedule.
