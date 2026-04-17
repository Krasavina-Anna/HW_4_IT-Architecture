import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta
import sqlite3


# ---------------- MOCK KAFKA ----------------
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


# ---------------- УВЕДОМЛЕНИЯ (3.5) ----------------
def notification_service(notifications_topic):
    print("Сервис уведомлений запущен\n")

    while True:
        message = notifications_topic.consume()
        if message:
            user_id = message.get("user_id")
            transaction_id = message.get("transaction_id")

            if "fraud_reason" in message:
                print(f"[УВЕДОМЛЕНИЕ] Пользователь {user_id}: операция {transaction_id} ЗАБЛОКИРОВАНА. Причина: {message['fraud_reason']}")
            else:
                print(f"[УВЕДОМЛЕНИЕ] Пользователь {user_id}: операция {transaction_id} выполнена успешно")

        time.sleep(0.2)


# ---------------- УЧЕТНАЯ СИСТЕМА (3.6) ----------------
def init_db():
    conn = sqlite3.connect("ledger.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id TEXT,
        user_id TEXT,
        amount REAL,
        type TEXT,
        timestamp TEXT
    )
    """)

    conn.commit()
    conn.close()


def ledger_service(ledger_topic):
    conn = sqlite3.connect("ledger.db")
    cursor = conn.cursor()

    print("Учётная система запущена\n")

    while True:
        message = ledger_topic.consume()
        if message:
            cursor.execute("""
            INSERT INTO transactions VALUES (?, ?, ?, ?, ?)
            """, (
                message["transaction_id"],
                message["user_id"],
                message["amount"],
                message["type"],
                message["timestamp"]
            ))
            conn.commit()

            print(f"[LEDGER] Сохранена транзакция {message['transaction_id']}")

        time.sleep(0.2)


# ---------------- CRM (3.7) ----------------
def crm_service(crm_topic):
    print("CRM сервис запущен\n")

    user_transactions = defaultdict(list)

    while True:
        message = crm_topic.consume()
        if message:
            user_id = message["user_id"]
            amount = message["amount"]
            timestamp = datetime.fromisoformat(message["timestamp"])

            user_transactions[user_id].append((timestamp, amount))

            cutoff = datetime.now() - timedelta(minutes=5)
            user_transactions[user_id] = [
                (t, a) for t, a in user_transactions[user_id] if t > cutoff
            ]

            total = sum(a for _, a in user_transactions[user_id])

            print(f"[CRM] Пользователь {user_id}, сумма за 5 минут: {total}")

            if total > 200_000:
                print(f"[CRM] Пользователь {user_id} получил повышенный статус")

        time.sleep(0.2)


# ---------------- ТОПИКИ ----------------
notifications = MockKafkaTopic("notifications")
ledger_events = MockKafkaTopic("ledger.events")
crm_updates = MockKafkaTopic("crm.updates")


# ---------------- MAIN ----------------
if __name__ == "__main__":
    init_db()

    # тестовые данные
    notifications.send({
        "transaction_id": "tx_1",
        "user_id": "user_1"
    })

    notifications.send({
        "transaction_id": "tx_2",
        "user_id": "user_2",
        "fraud_reason": "Сумма превышает лимит"
    })

    ledger_events.send({
        "transaction_id": "tx_3",
        "user_id": "user_3",
        "amount": 5000,
        "type": "transfer",
        "timestamp": datetime.now().isoformat()
    })

    crm_updates.send({
        "transaction_id": "tx_4",
        "user_id": "user_1",
        "amount": 150000,
        "type": "purchase",
        "timestamp": datetime.now().isoformat()
    })

    crm_updates.send({
        "transaction_id": "tx_5",
        "user_id": "user_1",
        "amount": 60000,
        "type": "purchase",
        "timestamp": datetime.now().isoformat()
    })

    threading.Thread(target=notification_service, args=(notifications,), daemon=True).start()
    threading.Thread(target=ledger_service, args=(ledger_events,), daemon=True).start()
    threading.Thread(target=crm_service, args=(crm_updates,), daemon=True).start()

    print("Все сервисы запущены\n")

    while True:
        time.sleep(1)