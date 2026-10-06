# ig-connector

Коннектор личных аккаунтов Instagram (Direct) к CRM через шину Kafka. CRM шлёт команды
(подключить Аккаунт, найти получателя, отправить текст), коннектор исполняет их от имени
Аккаунта через private API Instagram (библиотека aiograpi) и публикует ответы и события:
входящие сообщения, смену статуса Аккаунта. Каждый Аккаунт ходит в Instagram только через
свой прокси от сервиса прокси, напрямую никогда. Протокол: контракт шины 2.1
([docs/kafka-contract-2.1.md](docs/kafka-contract-2.1.md)); действующая версия контракта
2.2 ([docs/kafka-contract-2.2.md](docs/kafka-contract-2.2.md)) совместима с 2.1, что из неё
ещё не перенесено, ниже.

Дальше:

- [docs/architecture.md](docs/architecture.md): модули, жизненный цикл команды, вход,
  отправка, входящие, статусы, прокси, хранение, тесты.
- [docs/capabilities.md](docs/capabilities.md): возможности канала (§10 контракта) и
  матрица «работает / ещё нет / Instagram или aiograpi не умеет».

Словарь: **Источник** (source в контракте, `source_id`) — канал в CRM; **Аккаунт** —
аккаунт Instagram, привязанный к Источнику; **Сессия** — залогиненное состояние aiograpi
Аккаунта; **Флоу входа** — `connect.start` + `connect.confirm`; **Операция** — команда CRM с
сохранённым исходом.

## Схема

```mermaid
flowchart LR
    CRM["CRM"]
    Kafka[("Kafka<br/>топики команд и событий")]
    Conn["ig-connector<br/>(один процесс)"]
    PG[("PostgreSQL<br/>Операции, Сессии, позиции")]
    PS["Сервис прокси"]
    PX["Прокси Аккаунта"]
    IG["Instagram"]
    S3[("S3 шины")]

    CRM -->|команды| Kafka
    Kafka -->|события, ack, result| CRM
    Kafka -->|команды| Conn
    Conn -->|события, ack, result| Kafka
    Conn --> PG
    Conn -->|резерв, heartbeat, переезд| PS
    Conn -->|aiograpi, Chromium при входе| PX
    PX --> IG
    Conn -.->|вложения: этап 2| S3
    CRM -.-> S3
```

S3 в этапе 1 настраивается (доступ проверяют интеграционные тесты), но не используется:
вложений пока нет.

## Что умеет (этап 1)

- **Подключение Аккаунта.** Флоу входа в два шага: `connect.start` с логином и требованием к
  прокси, `connect.confirm` с паролем и TOTP-секретом. Вход через веб-версию Instagram в
  безголовом Chromium через прокси Аккаунта, затем `sessionid` передаётся в aiograpi.
  Устройство (профиль браузера, настройки aiograpi) создаётся один раз и переиспользуется.
  Переподключение того же Источника (`reconnect_source_id`). Один Аккаунт = один Источник:
  второй Источник на тот же Аккаунт отклоняется, кроме случая, когда прежний Источник мёртв
  (тогда Аккаунт переезжает, а прежний получает `disabled`).
- **Отправка текста** (`command.send`) в существующий личный диалог: `ack` сразу после
  сохранения Операции, ровно один `result` на доставку, без дублей при падении посреди
  отправки (своя метка `client_context`, сверка с историей диалога, иначе
  `send_unconfirmed`).
- **Поиск получателя** (`command.resolve_recipient`) по username и Instagram ID, в пределах
  10 секунд CRM; по телефону честный `recipient_not_found`.
- **Входящие тексты** личных диалогов (`event.inbound_message`): опрос Direct по HTTP раз в
  `INBOUND_POLL_INTERVAL` с позицией на диалог, `occurred_at` = время сообщения в Instagram,
  ничего не отмечается прочитанным. Нетекстовые входящие приходят с `kind: unsupported`.
  Истории до подключения не загружается.
- **Статусы** (`event.status`): только по факту от Instagram и только при смене, включая
  возврат в `active`. При старте процесса `active` не рассылается, его даёт проверка Сессии.
- **Рестарт без повторного входа:** Сессии зашифрованы в PostgreSQL и поднимаются после
  старта через тот же прокси.
- **Прокси** (§8 контракта): резерв по требованию из `connect.start`, heartbeat раз в 60 с,
  плановый переезд, переключение при подтверждённом сетевом сбое. Нет прокси = в сеть не
  ходим, `source_offline`.
- `command.edit` → `edit_unsupported`, `command.delete` → `channel_rejected`.

## Разработка

Нужны Python 3.12, [uv](https://docs.astral.sh/uv/) и Docker (для PostgreSQL).

```bash
uv sync
cp .env.example .env
```

### Тесты

```bash
uv run pytest                    # unit, contract, behaviour, adapter, weblogin
uv run pytest -m integration     # против стенда шины из .env
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

Поведенческим тестам (`tests/behaviour/`) нужен PostgreSQL: каждый тест берёт свою схему и
удаляет её после себя. Без базы они пропускаются с подсказкой. Достаточно одноразового
контейнера:

```bash
docker run -d --rm --name ig-pg -e POSTGRES_PASSWORD=dev -p 5432:5432 postgres:17-alpine
# в .env:
# DATABASE_URL=postgresql://postgres:dev@localhost:5432/postgres
# SESSION_ENCRYPTION_KEY=<uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
```

Тесты `tests/weblogin/` запускают настоящий Chromium и без него пропускаются: один раз
`uv run playwright install chromium`. Каждый поведенческий тест пишет трассу в `.test-runs/<запуск>/<тест>/`
(`trace.jsonl` и читаемый `summary.md`), секреты в ней замаскированы; хранятся последние 10
запусков.

### Тестовый стенд шины

Для интеграционных тестов и ручной проверки нужен стенд: Kafka с SASL SCRAM-SHA-256 и
топиками `crm.connector.commands.<channel_type>` / `crm.connector.events.<channel_type>`,
S3-совместимое хранилище и PostgreSQL. Доступы стенда кладутся в `.env` так же, как на
сервере (таблица переменных ниже), плюс `FAKE_CRM_KAFKA_*`: принципал с правом записи в
топик команд, у коннектора такого права нет. Интеграционные тесты идут против этого `.env`,
не против прода.

### Сервис и fake-crm

```bash
uv run ig-connector          # до SIGTERM/SIGINT; миграции базы при старте, логи JSON в stdout
uv run ig-connector health   # 0 = здоров, 1 = нет
```

`fake-crm` играет роль CRM: пишет команды в топик команд и печатает события коннектора,
проверяя каждое по моделям контракта. Пример: команда → `ack` → `result`.

```bash
# терминал 1
uv run ig-connector

# терминал 2
uv run fake-crm listen

# терминал 3: поиск получателя от имени Источника, которого коннектор не знает
uv run fake-crm resolve --source-id "$(uuidgen)" --kind username --value instagram
```

`fake-crm resolve` печатает отправленную команду, `listen` показывает ответы с тем же
`operation_id`:

```text
12:00:01 ack op=5c1f…
  {}
12:00:01 result op=5c1f…
  {"ok":false,"error_code":"source_offline","error_text":"the account has no working session"}
```

`ack` значит «Операция сохранена», `result` — исход. `source_offline` здесь честный: у
Источника нет Сессии, и CRM повторила бы команду по своему расписанию. Повторная отправка
той же команды (тот же `operation_id`) даёт новый `ack` и новый `result`, а успешный исход
вернулся бы из базы без повторного действия.

Остальные команды:

```bash
uv run fake-crm send --source-id <uuid> --chat <external_chat_id> --text "привет"
uv run fake-crm connect --phone +491701234567
uv run fake-crm confirm --operation-id <operation_id из connect> --password <пароль>
```

`fake-crm connect` шлёт `proxy_country_code: null` («напрямую» по контракту), поэтому
коннектор отвечает `event.connect.status` `failed` + `source_offline`: так проверяется отказ
без прокси. Для настоящего входа нужна команда с `proxy_country_code`, `login` и
`totp_secret` (поля ниже, в «Подключение Аккаунта»); `fake-crm` их пока не задаёт.

## Развёртывание (docker compose)

В комплекте `Dockerfile` (Python 3.12, зависимости из `uv.lock`, Chromium для входа
через браузер, процесс под непривилегированным пользователем `connector`) и
`docker-compose.yml`: коннектор и его собственный PostgreSQL 17. Нужны Docker с
плагином compose и исходящий доступ с сервера к Kafka и S3 шины, к сервису прокси и к
самим прокси. Входящих портов коннектор не открывает. Образ около 1,1 ГБ на диске
(400 МБ при скачивании), из них Chromium с системными библиотеками около 600 МБ.

### Что нужно до запуска

1. **Комплект доступов к шине** (контракт, §2) от команды CRM: адреса брокеров Kafka,
   логин и пароль SCRAM-SHA-256 коннектора, `channel_type`, endpoint, бакет и ключи S3.
2. **Токен сервиса прокси** (`x-proxy-token`) и его базовый URL. Без прокси коннектор
   ни один Аккаунт не подключает и в Instagram не ходит: напрямую он не выходит никогда.
   Подключение тогда отвечает `failed` + `source_offline`, а уже подключённые Аккаунты
   получают `event.status` `error` + `source_offline`.
3. **Ключ шифрования Сессий.** Сгенерировать один раз (после `docker compose build`):

   ```bash
   docker run --rm --entrypoint python ig-connector:latest \
     -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

   (или любым Python с пакетом `cryptography`). Сессии Instagram и устройства лежат в
   базе только зашифрованными этим ключом. **Потеря или замена ключа = потеря всех
   Сессий**: все Аккаунты придётся подключать заново, а каждый новый вход Instagram
   может встретить проверкой. Храните ключ в хранилище секретов отдельно от бэкапов базы.
4. **Пароль PostgreSQL**: любой случайный, без спецсимволов URL, например
   `openssl rand -hex 24`.

### Переменные окружения

Скопировать `.env.example` в `.env` рядом с `docker-compose.yml` и заполнить.
Конфиг берётся только из окружения; при ошибке в нём коннектор пишет в лог имена
неверных полей (без значений) и завершается с кодом 2.

| Переменная | Откуда |
|---|---|
| `CHANNEL_TYPE` | комплект доступов: имя канала, из него имена топиков |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_USERNAME`, `KAFKA_PASSWORD` | комплект доступов (SASL SCRAM-SHA-256) |
| `S3_ENDPOINT`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | комплект доступов |
| `PROXY_SERVICE_URL`, `PROXY_SERVICE_TOKEN` | сервис прокси (контракт, §8) |
| `STATIC_PROXY_URL` | вместо сервиса прокси: один прокси на все Аккаунты, `scheme://user:password@host:port` (для стенда) |
| `SESSION_ENCRYPTION_KEY` | сгенерировать (выше) |
| `POSTGRES_PASSWORD` | придумать (выше); compose сам собирает из него `DATABASE_URL` |
| `DATABASE_URL` | оставить пустым; заполнять, только если база своя, вне compose |
| `MAX_PARALLEL_SOURCES` | сколько Аккаунтов одновременно обращаются к Instagram (команды, опрос входящих, восстановление), по умолчанию 20 |
| `INBOUND_POLL_INTERVAL` | раз во сколько секунд опрашивать входящие каждого Аккаунта (не меньше 5), по умолчанию 30 |
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` / `ERROR`, по умолчанию `INFO` |
| `HEALTH_FILE` | не задавать: файл для healthcheck внутри контейнера |
| `FAKE_CRM_KAFKA_*` | только для тестового стенда, на сервере не нужны |

### Запуск, остановка, обновление

```bash
docker compose up -d --build        # собрать образ и запустить
docker compose ps                   # коннектор должен стать healthy за ~30 с
docker compose logs -f connector    # логи
docker compose stop                 # остановить (данные и Сессии остаются)
git pull && docker compose up -d --build   # обновить: новый образ, миграции базы при старте
```

`docker compose down -v` удаляет том базы, а с ним все Сессии и историю операций: так
делать только при полном выводе из эксплуатации. Бэкап = дамп базы
(`docker compose exec postgres pg_dump -U connector connector`) плюс ключ
`SESSION_ENCRYPTION_KEY`, одно без другого бесполезно.

Коннектор работает в одном экземпляре: Сессия Аккаунта живёт ровно в одном процессе.
Не масштабировать (`--scale`) и не запускать второй compose с тем же `CHANNEL_TYPE`.

Healthcheck (`ig-connector health`) зелёный, пока база отвечает и консьюмер Kafka держит
свои партиции (файл `HEALTH_FILE` обновляется раз в 10 с, старше 30 с = нездоров).
Нездоров дольше 30 с = смотреть логи: обычно недоступна Kafka или база.

### Рестарт

- Процесс завершается сам, когда теряет партиции Kafka (например, брокер перезапустился)
  или конфиг неверен; `restart: unless-stopped` поднимает его снова.
- При остановке коннектор перестаёт брать новые команды и даёт текущим до 8 с
  закончиться. Незавершённые команды не подтверждены в Kafka и придут снова после
  старта; отправка, которая могла уже уйти, повторно не отправляется (сверка с
  Instagram, иначе `send_unconfirmed`).
- После старта коннектор поднимает Сессию каждого Аккаунта из базы через его прокси и
  проверяет её. `event.status active` уходит только после успешной проверки, при старте
  статусы не рассылаются. Отозванная Instagram Сессия = статус ошибки, Аккаунт
  подключают заново (ниже, с `reconnect_source_id`).

### Подключение Аккаунта

Подключает оператор из CRM. Вход по логину и паролю Instagram идёт в два шага; поля
`login` и `totp_secret` пока наше предложение к контракту, CRM может их переименовать.

1. `command.connect.start`: `method: "pairing_code"`, `login` (username, email или
   телефон Instagram) и требование к прокси: `proxy_country_code` обязателен (`null` =
   «напрямую», это коннектор отклоняет с `source_offline`), `proxy_network_type` по
   желанию. Коннектор резервирует прокси и отвечает `event.connect.status` `confirming`.
2. В течение 5 минут `command.connect.confirm` с тем же `operation_id`: `password` и,
   если на аккаунте включена 2FA через приложение-аутентификатор, `totp_secret`
   (ключ настройки 2FA в base32, как его показывает Instagram; коннектор сам
   генерирует коды). Пароль и ключ нигде не сохраняются и не логируются.
3. Коннектор входит через браузер (Chromium) через прокси Аккаунта и отвечает
   `event.connect.status` `done` (идентификатор и username аккаунта) или `failed` с
   кодом: `verify_failed` (неверный пароль или ключ), `connect_failed` (Instagram просит
   проверку: пройти её в приложении Instagram и повторить; прочие отказы),
   `source_offline` (нет прокси или он не работает), `peer_flood`.

Переподключение того же Аккаунта: `connect.start` с `reconnect_source_id`, `login` можно
не передавать. Попыток входа лучше не больше одной в сутки на Аккаунт: частые входы
Instagram наказывает проверками.

### Логи

Одна JSON-строка на событие в stdout (`docker compose logs connector`): `time`, `level`,
`logger`, `message` и поля события (`source_id`, `operation_id`, `command`,
`error_code`, `duration_ms`, ...). Текстов переписки в логах нет, только длина; пароли,
коды, Сессии, креды прокси и S3 маскируются. Ротация: 5 файлов по 20 МБ (docker
json-file, настраивается в `docker-compose.yml`).

## Ограничения

- **Один процесс на канал.** Сессия Аккаунта живёт в памяти одного процесса; второй
  экземпляр с тем же `CHANNEL_TYPE` сломает и порядок команд, и Сессии. Предел по нагрузке
  задаёт `MAX_PARALLEL_SOURCES`.
- **Только текст в существующий личный диалог.** Вложения в исходящих → `channel_rejected`
  «attachments are not supported yet». Первое сообщение человеку без диалога →
  `channel_rejected` «starting new dialogs is stage 3». `reply_to_external_id` принимается,
  но сообщение уходит без цитаты.
- **Входящие:** только личные диалоги основного инбокса. Групповые чаты и запросы на
  переписку (pending) пропускаются; нетекстовые сообщения (фото, видео, голосовые,
  пересылки) приходят как `kind: unsupported` без вложения. Задержка входящих до
  `INBOUND_POLL_INTERVAL` (опрос, не realtime). Свои сообщения Аккаунта, отправленные из
  приложения, в CRM не попадают.
- **Вход:** только логин + пароль + TOTP-секрет. Проверки Instagram (SMS, почта, чекпоинт,
  капча, 2FA без секрета) коннектор не проходит: `connect_failed` с подсказкой пройти
  проверку в приложении. Рестарт посреди входа = `failed` «login interrupted», вход не
  повторяется.
- **Прокси для входа:** HTTP(S) или SOCKS5 без авторизации. Chromium не умеет SOCKS5 с
  логином и паролем, такой прокси даёт `connect_failed` «proxy scheme not supported for
  login». Уже подключённые Аккаунты работают через любой прокси.
- **Голосовые** не поддерживаются ни во входящих, ни в исходящих: контракт 2.1 их
  исключает.
- **Живой Instagram не проверен.** Вход, отправка и приём против настоящего Instagram через
  рабочие прокси ещё не прогонялись: у сервиса прокси пустой пул. Проверено на фейках
  (Instagram, сервис прокси, веб-вход), на настоящем aiograpi с подменённым HTTP-транспортом
  и на тестовом стенде шины.

Полная матрица возможностей с причинами: [docs/capabilities.md](docs/capabilities.md).

## Контракт 2.2: что ещё не перенесено

Код реализует семантику 2.1 и ставит `contract_version: "2.1"` в свои события. 2.2 не
переименовывает полей и не делает обязательным необязательное, поэтому CRM на 2.2 такой
коннектор принимает. Не перенесено:

- **Вход челленджами** (`method: login_password`, `event.connect.status` `state: challenge`
  с `kind` `password` / `totp` / `code` / `manual`, ответы `password` / `totp` в
  `connect.confirm`, остановка пятиминутного счётчика на челлендже). Сейчас вход одним
  `connect.confirm` с `password` + `totp_secret` (наше расширение 2.1).
- **Импорт готовой Сессии** (`method: session`, объект в бакете под `crm/`) и код
  `session_invalid`.
- **`rate_limited` + `reason`** вместо `peer_flood`: коннектор шлёт `peer_flood` (в `result` и
  в `event.status`), CRM 2.2 его ещё принимает и приводит сама.
- **Поля `event.status`** `reason`, `resume_at`, `action_required`: не заполняются. Проверка,
  которую Instagram требует от работающего Аккаунта, сейчас `needs_reconnect` +
  `deauthorized`, а не пауза с `action_required`.
- **`already_connected`** и **`access_restricted`** во входе: занятый Аккаунт сейчас
  `connect_failed` с текстом.
- **`not_supported`**: вложения, `delete` и новый диалог отвечают `channel_rejected`.
- **`command.disconnect`**: не разбирается (`can_disconnect` не объявлен, CRM её не шлёт).
  Выключение Источника в CRM коннектор не видит: Сессия остаётся живой, входящие
  опрашиваются, прокси держится.
- **`idempotency_conflict`**: содержание команды не сохраняется, поэтому повтор
  `(operation_id, type)` с другим содержанием не отличить: он отвечается как повтор.
- **Входящие:** `direction`, `sent_at`, `status` у вложений (`media[]` с `status:
  unsupported` вместо пустого `kind: unsupported`).
- **Несколько реплик** (§3 2.2): не планируются, коннектор остаётся в одном процессе.

## Структура

- `src/ig_connector/contract/`: модели контракта 2.1 (конверт, команды, события, коды ошибок)
- `src/ig_connector/runtime/`: ядро: очередь и исполнитель на Источник, Флоу входа,
  отправка, поиск, входящие, статусы, восстановление Сессий, жизненный цикл прокси
- `src/ig_connector/instagram/`: порт платформы; `adapter/` на aiograpi, `weblogin/` вход
  через Chromium
- `src/ig_connector/proxy/`: порт прокси, клиент сервиса прокси, статический прокси
- `src/ig_connector/store/`: порт хранилища, PostgreSQL и SQL-миграции
- `src/ig_connector/bus/`: порт шины и Kafka
- `src/ig_connector/app.py`: сборка сервиса из env, запуск, остановка, healthcheck
- `src/ig_connector/devtools/`: fake-crm
- `tests/contract/`: модели против всех JSON-примеров документа контракта
- `tests/behaviour/`: коннектор целиком на шине в памяти, фейковых Instagram и прокси
- `tests/adapter/`, `tests/weblogin/`: настоящий aiograpi и Chromium против подставных
  серверов
- `tests/integration/`: стенд шины (Kafka, S3, PostgreSQL)
