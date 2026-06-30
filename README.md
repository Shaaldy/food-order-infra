# food-order

Event-driven микросервисная система обработки заказов на **Spring Boot 4.1 / Java 21**. Демонстрирует асинхронное взаимодействие сервисов через Kafka, транзакционную публикацию событий (Outbox), choreography-сагу с компенсирующими транзакциями, многоуровневую идемпотентность и observability на базе OpenTelemetry.

Это инфраструктурный репозиторий — **точка входа в проект**. Он оркеструет все сервисы через единый `docker-compose` и описывает систему целиком. Сами сервисы живут в отдельных репозиториях (осознанный выбор в пользу настоящей микросервисной изоляции — см. ниже).

| Репозиторий | Назначение |
|---|---|
| [order-service](https://github.com/Shaaldy/order-service) | Приём заказов (REST), транзакционная публикация событий через Outbox, обновление статуса по результату оплаты/возврата |
| [payment-service](https://github.com/Shaaldy/payment-service) | Идемпотентная обработка платежей и возвратов (ledger), реакция на события заказов |
| [food-order-infra](https://github.com/Shaaldy/food-order-infra) | docker-compose: Redpanda + 2× PostgreSQL + оба сервиса + стек observability (этот репозиторий) |

---

## Архитектура

Два событийных конвейера между сервисами: прямой путь оплаты и компенсирующий путь отмены/возврата. Сервисы **не вызывают друг друга напрямую** — связь только через Kafka-топики, что даёт слабую связанность и отказоустойчивость (падение Payment не роняет приём заказов).

```
  ── Прямой путь (оплата) ────────────────────────────────────────────────

      Клиент ─POST /api/orders─▶ Order Service ──order.created──▶ Payment Service
                                      ▲                                │
                                      │                                │ обработка платежа
                                      └────────payment.processed───────┘ (идемпотентно)
                                         (заказ → PAID / PAYMENT_FAILED)

  ── Компенсирующий путь (сага отмены) ───────────────────────────────────

      Клиент ─POST .../cancel─▶ Order Service ──order.cancelled──▶ Payment Service
                                      ▲                                │
                                      │                                │ возврат средств
                                      └────────payment.refunded────────┘ (запись в ledger,
                                         (заказ → CANCELLED)               идемпотентно)


   Order DB (PostgreSQL)        ◇ изоляция данных на сервис ◇        Payment DB (PostgreSQL)
   + таблица outbox                  общего хранилища нет             payments + refunds (ledger)
   (транзакционная публикация)
```

**Топики:** `order.created`, `payment.processed`, `order.cancelled`, `payment.refunded`.

**Прямой поток:**
1. `POST /api/orders` → заказ сохраняется со статусом `CREATED`. В **той же транзакции** в таблицу `outbox` пишется `OrderCreatedEvent` (см. раздел Outbox).
2. Фоновый relay вычитывает outbox и публикует событие в `order.created`.
3. Payment Service слушает `order.created`, обрабатывает платёж (идемпотентно), публикует `PaymentProcessedEvent` в `payment.processed`.
4. Order Service слушает `payment.processed` и переводит заказ в `PAID` либо `PAYMENT_FAILED`.

**Компенсирующий поток (сага):**
1. `POST /api/orders/{id}/cancel` → Order через state machine решает, допустима ли отмена, и публикует `OrderCancelledEvent` в `order.cancelled`.
2. Payment Service слушает `order.cancelled`, создаёт запись о возврате в ledger (идемпотентно) и публикует `PaymentRefundedEvent` в `payment.refunded`.
3. Order переводит заказ в `CANCELLED`.

Полный асинхронный цикл оплаты — создание заказа → публикация → обработка → обновление статуса — проходит end-to-end в районе десятков миллисекунд.

---

## Ключевые инженерные решения

Раздел сознательно вынесен наверх — это и есть суть проекта.

- **Event-driven, без прямых вызовов.** Order и Payment общаются исключительно через Kafka-топики. Никакого синхронного HTTP между сервисами.

- **Минимальные контракты событий.** `OrderCreatedEvent(orderId, amount)` намеренно НЕ тащит `items`/`customerId`/`status` — только то, что нужно платежу. `PaymentProcessedEvent(paymentId, orderId, success)` использует `boolean`, а не enum `PaymentStatus`, чтобы Order не зависел от внутренних типов Payment. Слабая связанность через узкий контракт.

- **Transactional Outbox (закрывает dual-write).** Раньше Order сохранял заказ и публиковал событие двумя отдельными операциями (`saveAndFlush` + `KafkaTemplate.send`) без общей транзакции — упади процесс между ними, и заказ есть, а события нет. Решение: запись заказа и запись в таблицу `outbox` происходят в **одной транзакции БД**. Фоновый `@Scheduled(fixedDelay)` relay вычитывает outbox и публикует в Kafka, гарантируя at-least-once доставку. Отправленные записи удаляются (DELETE-стратегия вместо флага — прагматичный выбор для масштаба проекта). Дубликаты, возможные при at-least-once, гасятся идемпотентностью на стороне consumer'а.

- **Choreography Saga отмены и возврата (компенсация ≠ rollback).** Возврат средств — это **новое семантическое действие**, а не откат предыдущего. Реализована хореографическая сага: `OrderService.cancel()` — это state machine на пять веток (что делать с `CREATED`, `PAID`, уже `CANCELLED` и т.д.), которая публикует `order.cancelled`; Payment реагирует возвратом. Хореография (события, а не центральный оркестратор) выбрана осознанно как естественная для двух сервисов.

- **Ledger-модель возвратов.** Возврат фиксируется отдельной append-only записью `Refund` с самодостаточной суммой, а не мутацией исходного платежа. `Refund` хранит `paymentId` как обычный UUID (а не `@OneToOne`) — запись ledger самодостаточна и не зависит от связности с платежом. Это ближе к тому, как устроен реальный финансовый учёт.

- **Идемпотентность на двух уровнях — и для платежа, и для возврата.** Защита от at-least-once доставки Kafka: программный guard (`existsByOrderId` / проверка наличия возврата) **плюс** UNIQUE-констрейнт в БД. Программная проверка отсекает большинство дублей дёшево; констрейнт БД — последний рубеж на гонках. Повторное событие с тем же ключом не создаёт дубль ни платежа, ни возврата.

- **String-сериализация + ручной маппинг (решение, эволюционировавшее под давлением).** Сейчас все сервисы используют `StringSerializer`/`StringDeserializer`, а listener сам делает `objectMapper.readValue(json, EventType.class)` (Jackson 3). Это итог трёх итераций:
  1. Начали с `JsonSerializer`/`JsonDeserializer`. Получили cross-service `ClassNotFoundException`: сериализатор вшивает в заголовок полное имя класса со своим пакетом, а у consumer'а другой пакет. Залатали через `use.type.headers=false` + `value.default.type`.
  2. На Outbox всплыла **двойная сериализация**: outbox хранит уже готовую JSON-строку, а `KafkaTemplate` с `JsonSerializer` сериализовал её ещё раз.
  3. На саге consumer стал слушать **несколько типов событий** (`order.created` и `order.cancelled`), а `value.default.type` объявляет ровно один тип по умолчанию — подход с type-заголовками перестал работать в принципе.

  Переход на `String` + ручной `readValue` убирает все три проблемы разом: payload — это просто строка-байты, relay — чистый транспорт, а listener сам решает тип под конкретный топик. Талкинг-поинт: как требование (несколько типов событий) обнуляет ранее «достаточное» решение.

- **Изоляция данных на сервис.** У каждого сервиса своя PostgreSQL-база, общего хранилища нет — данные одного сервиса недоступны другому напрямую.

- **Observability (метрики + трейсинг).** Оба сервиса отдают метрики через Actuator + Micrometer в формате Prometheus (`/actuator/prometheus`, явный список эндпойнтов, не `*` — безопасность). Prometheus скрейпит, Grafana визуализирует (дашборд ID `19004`). Распределённый трейсинг: Micrometer Tracing → OpenTelemetry (OTLP/HTTP) → Grafana Tempo, спаны видны от обоих сервисов. Это даёт сквозную видимость событийной системы, где обычные стек-трейсы бесполезны.

- **CI на каждом сервисе.** GitHub Actions (`./mvnw verify` на `ubuntu-latest`, temurin 21): запускает и юниты (Surefire, `*Test`), и интеграционные тесты (Failsafe, `*IT`) на каждый push в `main` и pull request.

- **Тестирование с реальной инфраструктурой.** Интеграционные тесты поднимают настоящие Kafka и PostgreSQL через **Testcontainers** (не embedded, не моки), проверяя весь асинхронный путь end-to-end. Проверка *отсутствия* в async-системе делается через временно́е окно (`Awaitility.during`), а не мгновенный снимок; события читаются фильтром по `orderId`, а не «ровно одна запись в топике» (устойчиво к накоплению событий в общем брокере).

---

## Технологический стек

- **Язык / рантайм:** Java 21 (LTS)
- **Фреймворк:** Spring Boot 4.1, Spring Framework 7, Spring for Apache Kafka 4
- **Персистентность:** JPA / Hibernate, PostgreSQL, Flyway (миграции)
- **Брокер сообщений:** Apache Kafka (локально — Redpanda, Kafka-совместимый)
- **Observability:** Micrometer, Prometheus, Grafana, Grafana Tempo, OpenTelemetry (OTLP/HTTP)
- **Сборка:** Maven, Lombok
- **Тестирование:** JUnit 5, Mockito, AssertJ, **Testcontainers** (Kafka + PostgreSQL), Awaitility, RestTestClient
- **CI:** GitHub Actions (`./mvnw verify`, Failsafe для интеграционных тестов)
- **Качество кода:** Spotless (Google Java Format)

---

## Как запустить

Требуется установленный Docker.

```bash
git clone https://github.com/Shaaldy/food-order-infra.git
cd food-order-infra
docker compose up --build
```

Поднимется всё: Redpanda, две базы PostgreSQL, оба сервиса и стек observability (Prometheus, Grafana, Tempo). После старта Order Service доступен на `localhost:8080`.

**Внешние порты:**

| Компонент | Порт |
|---|---|
| Order Service (HTTP) | `8080` |
| Order PostgreSQL | `5432` |
| Payment PostgreSQL | `5433` |
| Redpanda (Kafka) | `19092` |
| Prometheus | `9090` |
| Grafana | `3000` |
| Tempo | `3200` |

Payment Service отдаёт только Actuator-эндпойнты (метрики/трейсинг) — бизнес-REST у него нет, он работает как Kafka-consumer.

После старта: Grafana на `localhost:3000` (метрики — дашборд `19004`, трейсы — data source Tempo).

---

## API (Order Service)

Базовый путь: `/api/orders`

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/api/orders` | Создать заказ. Возвращает `201 Created` с `Location` и телом заказа (статус `CREATED`) |
| `GET` | `/api/orders/{id}` | Получить заказ по id |
| `GET` | `/api/orders` | Список заказов (постранично, `Pageable`) |
| `POST` | `/api/orders/{id}/cancel` | Отменить заказ (запускает сагу возврата, если заказ был оплачен) |

### Пример

```bash
# Создать заказ
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
        "customerId": "...",
        "items": [
          { "productName": "Pizza", "price": 12.50, "quantity": 2 }
        ]
      }'

# Проверить статус (вскоре станет PAID или PAYMENT_FAILED)
curl http://localhost:8080/api/orders/{id}

# Отменить (если был PAID — Payment проведёт возврат через сагу)
curl -X POST http://localhost:8080/api/orders/{id}/cancel
```

Демо-логика оплаты детерминирована: чётная целая часть суммы → `SUCCESS`, нечётная → `FAILED` (заглушка платёжного шлюза; в проде здесь был бы реальный провайдер).

---

## Статусы заказа

```
                       ┌──────────────▶ PAID ─────────┐
   CREATED ────────────┤                              ├──────▶ CANCELLED
       │               └──────────▶ PAYMENT_FAILED    │      (через сагу возврата,
       │                                               │       если был PAID)
       └───────────────────────────────────────────────┘
                         cancel (до оплаты)
```

`cancel()` реализован как state machine: допустимость отмены и наличие компенсации (возврата) зависят от текущего статуса заказа. Отмена `PAID`-заказа запускает сагу возврата средств; отмена до оплаты просто закрывает заказ.

---

## Roadmap

Проект ведётся по стадиям; критический путь ориентирован на демонстрацию production-паттернов.

- [x] **Stage 1** — Order Service: REST, персистентность, валидация, тесты
- [x] **Stage 2** — Docker / docker-compose
- [x] **Stage 3** — Payment Service + Kafka (асинхронный поток, идемпотентность, полное тестовое покрытие)
- [x] **Stage 4** — Transactional Outbox (закрытие dual-write)
- [x] **Stage 5** — Choreography Saga (отмена + компенсирующий возврат, ledger, двухуровневая идемпотентность)
- [x] **Stage 8** — Observability (метрики + распределённый трейсинг)
- [x] **Stage 9** — CI (GitHub Actions, юниты + интеграционные тесты)
- [ ] **Stage 10** — Документация (этот README — финал ядра)

Бонусные стадии (после критического пути): Notification Service + Redis, Security / RBAC, Product Service (каталог как источник правды цен).

---

## Автор

Shaldy — [github.com/Shaaldy](https://github.com/Shaaldy)
