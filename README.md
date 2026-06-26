# food-order

Event-driven микросервисная система обработки заказов на **Spring Boot 4.1 / Java 21**. Демонстрирует асинхронное взаимодействие сервисов через Kafka, идемпотентную обработку платежей и production-релевантные паттерны интеграции данных.

Это инфраструктурный репозиторий — **точка входа в проект**. Он оркеструет все сервисы через единый `docker-compose` и описывает систему целиком. Сами сервисы живут в отдельных репозиториях (осознанный выбор в пользу настоящей микросервисной изоляции — см. ниже).

| Репозиторий | Назначение |
|---|---|
| [order-service](https://github.com/Shaaldy/order-service) | Приём заказов (REST), публикация событий, обновление статуса по результату оплаты |
| [payment-service](https://github.com/Shaaldy/payment-service) | Идемпотентная обработка платежей, реакция на события заказов |
| [food-order-infra](https://github.com/Shaaldy/food-order-infra) | docker-compose: Redpanda + 2× PostgreSQL + оба сервиса (этот репозиторий) |

---

## Архитектура

```
                 HTTP                          Kafka (Redpanda)
   Клиент ──POST /api/orders──▶ Order Service ──order.created──▶ Payment Service
                                      ▲                                │
                                      │                                │ обработка
                                      │                                │ (идемпотентно)
                                      └──────payment.processed─────────┘
                                         (PAID / PAYMENT_FAILED)

   Order DB (PostgreSQL)                          Payment DB (PostgreSQL)
   — изоляция данных на сервис, общего хранилища нет —
```

Полный асинхронный цикл — создание заказа → оплата → обновление статуса — проходит end-to-end примерно за 25 мс (POST → Kafka → обработка → запись в БД → публикация → обновление статуса заказа).

**Поток:**
1. `POST /api/orders` → заказ сохраняется со статусом `CREATED`, публикуется `OrderCreatedEvent` в топик `order.created`.
2. Payment Service слушает `order.created`, обрабатывает платёж (идемпотентно), публикует `PaymentProcessedEvent` в `payment.processed`.
3. Order Service слушает `payment.processed` и переводит заказ в `PAID` либо `PAYMENT_FAILED`.

Сервисы **не вызывают друг друга напрямую** — связь только через события, что даёт слабую связанность и отказоустойчивость (падение Payment не роняет приём заказов).

---

## Ключевые инженерные решения

Раздел сознательно вынесен наверх — это и есть суть проекта.

- **Event-driven, без прямых вызовов.** Order и Payment общаются исключительно через Kafka-топики. Никакого синхронного HTTP между сервисами.

- **Минимальные контракты событий.** `OrderCreatedEvent(orderId, amount)` намеренно НЕ тащит `items`/`customerId`/`status` — только то, что нужно платежу. `PaymentProcessedEvent(paymentId, orderId, success)` использует `boolean`, а не enum `PaymentStatus`, чтобы Order не зависел от внутренних типов Payment. Слабая связанность через узкий контракт.

- **Идемпотентность платежа на двух уровнях.** Защита от at-least-once доставки Kafka: `existsByOrderId` в логике сервиса + UNIQUE-констрейнт на `order_id` в БД. Повторное событие с тем же `orderId` отбрасывается, дубль платежа невозможен.

- **Изоляция данных на сервис.** У каждого сервиса своя PostgreSQL-база, общего хранилища нет — данные одного сервиса недоступны другому напрямую.

- **Cross-service десериализация (`use.type.headers=false`).** `JsonSerializer` по умолчанию вшивает в заголовок полное имя класса со своим пакетом. У разных сервисов пакеты разные → consumer не может создать чужой класс. Решено через `use.type.headers=false` + `value.default.type` на стороне consumer'а: «игнорируй подпись отправителя, считай всё в топике своим типом».

- **Тестирование с реальной инфраструктурой.** Интеграционные тесты поднимают настоящие Kafka и PostgreSQL через **Testcontainers** (не embedded, не моки), проверяя весь асинхронный путь end-to-end. Проверка отсутствия в async-системе делается через временно́е окно (`Awaitility.during`), а не мгновенный снимок; события читаются фильтром по `orderId`, а не «ровно одна запись в топике» (устойчиво к накоплению событий в общем брокере).

---

## Осознанные компромиссы (talking points)

Эти решения приняты намеренно — задокументированы как точки обсуждения, а не недоработки.

- **Слоистая архитектура вместо гексагональной.** Для пет-проекта гексагональная — оверинжиниринг. Паттерн понятен, выбрана слоистая ради простоты.

- **dual-write → Outbox (запланировано, Stage 4).** Сейчас Order сохраняет заказ и публикует событие без общей транзакции (`saveAndFlush` + publish). При падении между ними возможен заказ без события. Порядок «сначала БД, потом Kafka» = безопасный отказ. Полное решение — паттерн Outbox (следующая стадия). Промежуточный вариант (`@TransactionalEventListener(AFTER_COMMIT)`) рассмотрен и сознательно отложен в пользу Outbox.

- **Цена из запроса клиента.** Клиент задаёт цену позиции в запросе — нарушение принципа «не доверяй телу запроса». В норме цена берётся из каталога (Product Service как источник правды). Отложено, чтобы не уводить от ядра нарратива.

- **Spring Kafka 4.0: Json* → Jackson 3.** Фреймворк депрекейтнул `JsonSerializer`/`JsonDeserializer` в пользу Jackson-3-аналогов. Миграция не сделана сознательно: она затрагивает продакшн-сериализацию обоих сервисов и рискует сломать рабочий контракт; предупреждение подавлено. Различие между «заглушить варнинг» и «оценить радиус изменения».

---

## Технологический стек

- **Язык / рантайм:** Java 21 (LTS)
- **Фреймворк:** Spring Boot 4.1, Spring for Apache Kafka
- **Персистентность:** JPA / Hibernate, PostgreSQL, Flyway (миграции)
- **Брокер сообщений:** Apache Kafka (локально — Redpanda, Kafka-совместимый)
- **Сборка:** Maven, Lombok, MapStruct (order-service)
- **Тестирование:** JUnit 5, Mockito, AssertJ, **Testcontainers** (Kafka + PostgreSQL), Awaitility, RestTestClient
- **Инфраструктура:** Docker, docker-compose
- **Качество кода:** Spotless (Google Java Format)

---

## Как запустить

Требуется установленный Docker.

```bash
git clone https://github.com/Shaaldy/food-order-infra.git
cd food-order-infra
docker compose up --build
```

Поднимется всё: Redpanda, две базы PostgreSQL и оба сервиса. После старта Order Service доступен на `localhost:8080`.

**Внешние порты:**

| Компонент | Порт |
|---|---|
| Order Service (HTTP) | `8080` |
| Order PostgreSQL | `5432` |
| Payment PostgreSQL | `5433` |
| Redpanda (Kafka) | `19092` |

Payment Service не имеет HTTP-сервера — он работает как Kafka-consumer и не публикует REST-эндпоинтов.

---

## API (Order Service)

Базовый путь: `/api/orders`

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/api/orders` | Создать заказ. Возвращает `201 Created` с `Location` и телом заказа (статус `CREATED`) |
| `GET` | `/api/orders/{id}` | Получить заказ по id |
| `GET` | `/api/orders` | Список заказов (постранично, `Pageable`) |
| `POST` | `/api/orders/{id}/cancel` | Отменить заказ |

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

# Проверить статус (через ~25мс станет PAID или PAYMENT_FAILED)
curl http://localhost:8080/api/orders/{id}
```

Демо-логика оплаты детерминирована: чётная целая часть суммы → `SUCCESS`, нечётная → `FAILED` (заглушка платёжного шлюза; в проде здесь был бы реальный провайдер).

---

## Статусы заказа

`CREATED` → `PAID` | `PAYMENT_FAILED` → `CANCELLED`

---

## Roadmap

Проект ведётся по стадиям; критический путь ориентирован на демонстрацию production-паттернов.

- [x] **Stage 1** — Order Service: REST, персистентность, валидация, тесты
- [x] **Stage 2** — Docker / docker-compose
- [x] **Stage 3** — Payment Service + Kafka (асинхронный поток, идемпотентность, полное тестовое покрытие)
- [ ] **Stage 4** — Outbox pattern (закрытие dual-write)
- [ ] **Stage 5** — Saga (компенсирующие транзакции — напр. возврат средств при отмене оплаченного заказа)
- [ ] **Stage 8** — Observability
- [ ] **Stage 10** — Документация

Бонусные стадии (после критического пути): Product Service (каталог как источник правды цен), Security / RBAC.

---

## Автор

Shaldy — [github.com/Shaaldy](https://github.com/Shaaldy)
