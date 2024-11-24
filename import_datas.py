from pymongo import MongoClient, ReturnDocument
from datetime import datetime, timezone
from faker import Faker
import random
import mysql.connector
import time

# Підключення до MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["ChatDatabase"]

# Підключення до MySQL
mysql_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234567890",
    database="test_db"
)
mysql_cursor = mysql_connection.cursor()

# Колекції MongoDB
user_profiles = mongo_db["user_profiles"]
chats = mongo_db["chats"]
chat_members = mongo_db["chat_members"]

# Колекція для лічильників MongoDB
counters = mongo_db["counters"]

# Ініціалізація Faker для генерації випадкових даних
faker = Faker()


# Функція для отримання наступного ID з MongoDB
def get_next_id(sequence_name):
    counter = counters.find_one_and_update(
        {"_id": sequence_name},
        {"$inc": {"sequence_value": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return counter["sequence_value"]

# Функція для скидання лічильників у MongoDB
def reset_counters():
    for counter_name in ["user_profiles", "chats", "chat_members"]:
        counters.update_one(
            {"_id": counter_name},
            {"$set": {"sequence_value": 0}},
            upsert=True  # Створює документ, якщо він не існує
        )

# Функція для очищення таблиць у MongoDB
def reset_mongo_collections():
    for collection in [user_profiles, chats, chat_members]:
        collection.delete_many({})  # Очищає всі документи в колекції

# Функція для очищення MySQL таблиць
def reset_mysql_tables():
    tables = ["chat_members", "chats", "user_profiles"]
    for table in tables:
        mysql_cursor.execute(f"DELETE FROM {table}")
    mysql_connection.commit()

# Функція для додавання користувача в MongoDB і MySQL
def add_user_profile(user_name, password, email, modified_by=None):
    user_id = get_next_id("user_profiles")
    
    # Ensure user_name is unique
    mysql_cursor.execute("SELECT COUNT(*) FROM user_profiles WHERE user_name = %s", (user_name,))
    if mysql_cursor.fetchone()[0] > 0:
        user_name = f"{user_name}_{user_id}"
    
    # Ensure email is unique
    mysql_cursor.execute("SELECT COUNT(*) FROM user_profiles WHERE email = %s", (email,))
    if mysql_cursor.fetchone()[0] > 0:
        email = f"{email.split('@')[0]}_{user_id}@{email.split('@')[1]}"
    
    # Insert into MongoDB
    user = {
        "_id": user_id,
        "user_name": user_name,
        "password": password,
        "email": email,
        "is_deleted": False,
        "last_modified": datetime.now(timezone.utc),
        "modified_by": modified_by
    }
    user_profiles.insert_one(user)

    # Insert into MySQL
    mysql_cursor.execute("""
        INSERT INTO user_profiles (user_id, user_name, password, email, is_deleted, last_modified, modified_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (user_id, user_name, password, email, False, datetime.now(), modified_by))
    mysql_connection.commit()
    
    return user_id  # Return the actual inserted user_id

# Функція для створення чату в MongoDB і MySQL
def create_chat(chat_name, is_group, created_by):
    chat_id = get_next_id("chats")
    
    # MongoDB Insert
    chat = {
        "_id": chat_id,
        "chat_name": chat_name,
        "is_group": is_group,
        "created_by": created_by,
        "is_deleted": False,
        "last_modified": datetime.now(timezone.utc),
        "modified_by": created_by
    }
    chats.insert_one(chat)

    # MySQL Insert
    mysql_cursor.execute("""
        INSERT INTO chats (chat_id, chat_name, is_group, created_by, is_deleted, last_modified, modified_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (chat_id, chat_name, is_group, created_by, False, datetime.now(), created_by))
    mysql_connection.commit()
    
    return chat_id  # Return the inserted chat_id

# Функція для додавання учасника до чату в MongoDB і MySQL
def add_chat_member(chat_id, user_id, role="member", modified_by=None):
    chat_member_id = get_next_id("chat_members")
    
    # MongoDB Insert
    member = {
        "_id": chat_member_id,
        "chat_id": chat_id,
        "user_id": user_id,
        "role": role,
        "last_modified": datetime.now(timezone.utc),
        "modified_by": modified_by
    }
    chat_members.insert_one(member)

    # MySQL Insert
    mysql_cursor.execute("""
        INSERT INTO chat_members (chat_member_id, chat_id, user_id, role, last_modified, modified_by)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (chat_member_id, chat_id, user_id, role, datetime.now(), modified_by))
    mysql_connection.commit()

# Функція для заповнення всіх таблиць випадковими даними в MongoDB і MySQL
# Update the populate_all_tables function to use the returned chat_id
def populate_all_tables(records_count=1000):
    reset_counters()        # Скидання лічильників MongoDB
    reset_mongo_collections()  # Очищення колекцій MongoDB
    reset_mysql_tables()    # Очищення таблиць MySQL
    
    user_ids = []
    chat_ids = []

    # Generate and collect user IDs
    for _ in range(records_count):
        user_name = faker.user_name()
        password = faker.password()
        email = faker.email()
        user_id = add_user_profile(user_name, password, email)  # Collect the inserted user_id
        user_ids.append(user_id)

    # Generate random chats
    for _ in range(records_count // 5):  
        chat_name = faker.word()
        is_group = random.choice([True, False])
        created_by = random.choice(user_ids)
        chat_id = create_chat(chat_name, is_group, created_by)
        chat_ids.append(chat_id)

    # Generate random chat members
    for chat_id in chat_ids:
        for _ in range(random.randint(2, 10)):
            add_chat_member(
                chat_id=chat_id,
                user_id=random.choice(user_ids)  # Use valid user_id
            )

    print(f"Database populated: {records_count} users, {len(chat_ids)} chats.")

def test_performance(records_count=1000):
    # Тестування MongoDB
    start_time = time.time()
    populate_all_tables(records_count)  # Заповнення таблиць MongoDB
    mongo_duration = time.time() - start_time
    print(f"MongoDB: {mongo_duration:.4f} seconds")

    # Тестування MySQL
    reset_mysql_tables()  # Очищення таблиць MySQL
    start_time = time.time()
    populate_all_tables(records_count)  # Заповнення таблиць MySQL
    mysql_duration = time.time() - start_time
    print(f"MySQL: {mysql_duration:.4f} seconds")

# Порівняння швидкості виконання
    if mongo_duration < mysql_duration:
        print("MongoDB працює швидше.")
    elif mysql_duration < mongo_duration:
        print("MySQL працює швидше.")
    else:
        print("Час виконання однаковий для MongoDB та MySQL.")

# Основна функція для тестування
if __name__ == "__main__":
    populate_all_tables(records_count=1000)
    test_performance(records_count=1000)
