## Архитектура интеграционного решения на базе Apache Kafka

Проект реализует **событийно-ориентированную архитектуру (EDA)** с использованием Apache Kafka в качестве центральной шины сообщений. Система обрабатывает финансовые транзакции в реальном времени: проверяет их на мошенничество, обогащает и маршрутизирует в профильные сервисы (учёт, CRM, уведомления).

### Обзор компонентов

Решение состоит из следующих частей (в соответствии с пунктами задания):

| Пункт | Компонент | Файл реализации | Назначение |
|-------|-----------|----------------|-------------|
| 3.1 | Окружение (Kafka + Zookeeper) | `docker-compose.yml`, `3_1_and_3_2.ipynb` | Запуск брокера, создание 6 топиков |
| 3.2 | Генератор транзакций (Producer) | `3_1_and_3_2.ipynb` | Отправка случайных транзакций в `transactions.raw` |
| 3.3 | Антифрод-сервис (Consumer + Stream) | `3_3_and_3_4.ipynb` | Проверка правил, обогащение, фильтрация |
| 3.4 | Маршрутизатор (Content-Based Router) | `3_3_and_3_4.ipynb` | Направление сообщений по типу транзакции |
| 3.5 | Сервис уведомлений | `3_5 and 3_6 and 3_7.py` | Эмуляция отправки уведомлений в консоль |
| 3.6 | Учётная система (Ledger) | `3_5 and 3_6 and 3_7.py` | Сохранение транзакций в SQLite |
| 3.7 | CRM-сервис | `3_5 and 3_6 and 3_7.py` | Агрегация сумм за 5 минут, повышение статуса |

### Детали реализации каждого компонента

#### 3.1 Подготовка окружения
- **Docker Compose** (`docker-compose.yml`):
  - Zookeeper (порт 2181)
  - Kafka broker (порт 9092) с автоматическим созданием топиков (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`)
- **Создание топиков** в `3_1_and_3_2.ipynb` через `KafkaAdminClient`:
  - `transactions.raw`, `transactions.enriched`, `fraud.alerts`, `notifications`, `ledger.events`, `crm.updates`
  - Каждый топик: 1 партиция, фактор репликации 1.

#### 3.2 Генератор транзакций (Producer)
- **Pydantic-модель `Transaction`** с полями:
  - `transaction_id: str` (UUID по умолчанию)
  - `user_id: str`, `amount: float`, `currency: Literal["RUB","USD","EUR"]`
  - `type: Literal["purchase","transfer","payment"]`
  - `timestamp: datetime` (автоматически текущее время)
  - `merchant_id: Optional[str]`, `location: str`
- **Producer** (`KafkaProducer`):
  - Адрес: `localhost:9092`
  - Сериализатор: `lambda v: json.dumps(v, default=str).encode('utf-8')` — преобразует `datetime` и `UUID` в строки.
- **Генерация данных**:
  - 20 пользователей (`user_1` … `user_20`), 10 мерчантов, 5 локаций.
  - Суммы от 10 до 500 000 руб., распределение валют: 80% RUB, 15% USD, 5% EUR.
  - Типы транзакций выбираются равновероятно.
  - `merchant_id` может быть `None` с вероятностью 30%.
  - Задержка между отправками: случайная от 0.2 до 1 секунды.
- **Отправка**: 20 транзакций в топик `transactions.raw`.

#### 3.3 Антифрод-сервис (Consumer + Stream Processing)
Реализован в `3_3_and_3_4.ipynb` как класс `FraudDetectionService`.

- **Stateful-хранилище**: `self.user_transactions = defaultdict(list)` — для каждого пользователя список временных меток транзакций.
- **Правила проверки** (метод `check_rules`):
  1. `amount > 150 000` → подозрительно.
  2. `type == "transfer" and amount > 50 000` → подозрительно.
  3. **Оконное правило**: за последние 30 секунд у пользователя более 5 транзакций.
     - Сначала список меток очищается: оставляются только те, что `> now - timedelta(seconds=30)`.
     - Добавляется текущая метка.
     - Если длина списка `> 5`, то подозрительно.
- **Обогащение**:
  - Если чистая: добавляется `fraud_status="clean"`, `fraud_reason=None`.
  - Если подозрительная: добавляется `fraud_status="suspicious"` и `fraud_reason` с описанием.
- **Действия**:
  - Подозрительная транзакция отправляется в `fraud.alerts` (полный alert с `fraud_reason`) и в `notifications`.
  - Чистая транзакция отправляется в `transactions.enriched`.
- **Consumer** (`fraud_consumer`) в цикле читает из `transactions.raw` и вызывает `service.process(tx)`.

#### 3.4 Content-Based Router (Маршрутизатор)
Реализован в том же ноутбуке как класс `ContentBasedRouter`.

- **Логика маршрутизации** (метод `route`):
  - `type == "purchase"` → `crm.updates`
  - `type == "transfer"` → `ledger.events`
  - `type == "payment"` → одновременно `ledger.events` и `notifications` (publish-subscribe)
- **Consumer** (`router_consumer`) читает из `transactions.enriched` и вызывает `router.route(tx)`.

#### 3.5 Сервис уведомлений (Notification Service)
Реализован в `services.py` как функция `notification_service(notifications_topic)`.

- В бесконечном цикле вызывает `consume()` на топике.
- Если получено сообщение:
  - При наличии ключа `fraud_reason` → выводит в консоль:  
    `[УВЕДОМЛЕНИЕ] Пользователь {user_id}: операция {transaction_id} ЗАБЛОКИРОВАНА. Причина: {fraud_reason}`
  - Иначе → выводит:  
    `[УВЕДОМЛЕНИЕ] Пользователь {user_id}: операция {transaction_id} выполнена успешно`
- Задержка между итерациями 0.2 сек.

#### 3.6 Учётная система (Ledger Service)
Реализована в `services.py`:

- **`init_db()`**: создаёт SQLite-базу `ledger.db` и таблицу `transactions` с полями: `transaction_id`, `user_id`, `amount`, `type`, `timestamp`.
- **`ledger_service(ledger_topic)`**:
  - В цикле читает сообщения.
  - Вставляет запись в таблицу.
  - Выводит `[LEDGER] Сохранена транзакция {transaction_id}`.
  - Задержка 0.2 сек.

#### 3.7 CRM-сервис (Customer Relationship Management)
Реализован в `services.py` как функция `crm_service(crm_topic)`.

- **Stateful-хранилище**: `user_transactions = defaultdict(list)` — для каждого пользователя список кортежей `(timestamp, amount)`.
- **Оконная агрегация**:
  - При получении транзакции добавляется `(timestamp, amount)`.
  - Удаляются записи старше 5 минут: `if t > datetime.now() - timedelta(minutes=5)`.
  - Вычисляется сумма `amount` за последние 5 минут.
- **Логика повышения статуса**:
  - Если сумма > 200 000 руб. → выводится `[CRM] Пользователь {user_id} получил повышенный статус`.
- Задержка между итерациями 0.2 сек.

### Имитационная модель Kafka (`MockKafkaTopic`)

Поскольку на локальной машине не удалось развернуть Docker, а в Google Colab возникли проблемы с Faust, вся бизнес-логика отлаживалась на **имитационной модели**:

```python
class MockKafkaTopic:
    def __init__(self, name):
        self.name = name
        self.messages = []

    def send(self, message):
        self.messages.append(message)

    def consume(self):
        if self.messages:
            return self.messages.pop(0)
        return None
