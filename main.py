from pymongo import MongoClient
from random import choice, randint
from faker import Faker
import time
from bson.code import Code

# Объект Faker для генерации случайных данных в БД
from pyreadline.console import event

fake = Faker()

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["electronic_tickets"]


def check_db_and_collection(db_name: str, collection_name: str):
    """
    Проверка наличия базы данных и коллекции. При условии их отсутствия - создает их.
    """
    if db_name in client.list_database_names():
        print(f"База данных {db_name} уже существует.")
    else:
        print(f"База данных {db_name} не найдена. Создаем...")
        client[db_name]
    if collection_name in client[db_name].list_collection_names():
        print(f"Коллекция {collection_name} уже существует.")
    else:
        print(f"Коллекция {collection_name} не найдена. Создаем...")
        client[db_name][collection_name]
    return client[db_name][collection_name]


def create_events(collection, num_events: int):
    """
    Создание мероприятий со случайными данными.
    """
    formats = ["online", "offline", "hybrid"]
    statuses = ["new", "finished", "registration is over"]
    for _ in range(num_events):
        event = {
            "name": fake.word(),
            "date": fake.future_date(),
            "time": fake.time(),
            "address": {
                "city": {
                    "name": fake.city(),
                    "postal_code": fake.zipcode(),
                },
                "street": {
                    "name": fake.street_name(),
                    "building": fake.building_number()
                }
            },
            "available_seats": randint(),
            "age_limit": randint(10, 100),
            "format": choice(formats),
            "status": choice(statuses),
            "contacts": {
                "name": fake.name(),
                "email": fake.email(),
                "phone": fake.phone_number()
            }
        }
    collection.insert_one(event)


def create_members(collection, num_members: int):
    """
    Создание участников со случайными данными.
    """
    for _ in range(num_members):
        member = {
            "name": fake.name(),
            "age": randint(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "interests": [fake.word() for _ in range(randint(0, 6))],
            "event": fake.word()
        }
    collection.insert_one(member)


def create_orders(collection, num_orders: int):
    """
    Создание заказов электронных билетов со случайными данными.
    """
    formats = ["online", "offline"]
    for _ in range(num_orders):
        order = {
            "name": fake.name(),
            "event": fake.word(),
            "amount": randint(1, 100),
            "format": choice(formats),
            "email": fake.email(),
            "date_order": fake.date()
        }
    collection.insert_one(order)


def find_members_by_age_range(collection, min_age: int, max_age: int):
    """
    Поиск участников в указанном диапазоне возрастов.
    """
    results = collection.find({"$and": [{"age": {"$gte": min_age}},
                                        {"age": {"$lte": max_age}}]})
    return list(results)


def find_events_with_available_seats(collection, available_seats: int):
    """
    Поиск мероприятий с необходимым количеством свободных мест.
    """
    results = collection.find({"available_seats": {"$eq": available_seats}})
    return list(results)


def find_events_by_date(collection, date: str):
    """
    Поиска мероприятий на определенную дату.
    """
    results = collection.find({"date": {"$eq": date}})
    return list(results)


def find_orders_by_user(collection, name: str):
    """
    Поиск заказов указанного пользователя.
    """
    results = collection.find({"name": {"$eq": name}})
    return list(results)


def update_event_available_seats(collection, name: str, new_available_seats: int):
    """
    Обновление количества доступных мест на мероприятии.
    """
    collection.update_one({"name": name}, {"$set": {"available_seats": new_available_seats}})
    print(f"Количество доступных мест на мероприятие {name} успешно изменено на {new_available_seats}.")


def remove_docs_members_by_event(collection, name):
    """
    Удаление множества документов участников по названию мероприятия.
    """
    collection.remove({event: name})


def join_events_members(events_collection, members_collection):
    """
    Объединение коллекций мероприятий и участников по полю мероприятия.
    """
    pipeline = [
        {
            "$lookup":
                {
                    "from": members_collection,
                    "localField": "name",
                    "foreignField": "event",
                    "as": "members"
                }
        }
    ]
    results = events_collection.aggregate(pipeline)
    return list(results)


def sort_and_pagination_events_by_seats(collection, page, page_size):
    """
    Сортировка мероприятий по убыванию свободных мест, осуществление пагинации.
    """
    results = collection.find().sort("available_seats", -1).skip(page_size *
                                                                 (page - 1)).limit(page_size)
    return list(results)


def create_index_and_demonstrate(collection, field):
    """
    Создание индексов в коллекции collection по полю field.
    """
    collection.create_index(field)


def create_text_index(collection, field):
    """
    Создание текстового индекса в коллекции collection по полю field.
    """
    collection.create_index([(field, "text")])


def drop_all_indexes(collection):
    """
    Удаление всех индексов, созданных на коллекции.
    """
    print(f"\nDropping all indexes in the collection {collection}...")
    collection.drop_indexes()
    print(f"Indexes from {collection} were dropped successfully.")


def aggregate_orders_by_name():
    """
    Подсчет количества заказов по имени пользователя, сортировка в порядке убывания.
    """
    pipeline = [
        {"$group": {"_id": "$name", "sum": {"$sum": 1}}},
        {"$sort": {"sum": -1}}
    ]
    results = db.orders.aggregate(pipeline)
    for result in results:
        print(result)


def aggregate_orders_by_event():
    """
    Подсчет количества заказов по названию мероприятия, сортировка в порядке убывания.
    """
    pipeline = [
        {"$group": {"_id": "$event", "sum": {"$sum": 1}}},
        {"$sort": {"sum": -1}}
    ]
    results = db.orders.aggregate(pipeline)
    for result in results:
        print(result)


if __name__ == '__main__':

    # Проверка наличия и создание базы данных и коллекций
    events_collection = check_db_and_collection('electronic_tickets', 'events')
    members_collection = check_db_and_collection('electronic_tickets', 'members')
    orders_collection = check_db_and_collection('electronic_tickets', 'orders')

    # Создание мероприятий, участников и заказов билетов
    create_events(events_collection, 200)
    create_members(members_collection, 500)
    create_orders(orders_collection, 100)

    # Поиск всех участников от 18 до 50 лет включительно
    members_btw_18_50_years_old = find_members_by_age_range(members_collection, 18, 50)
    print(members_btw_18_50_years_old)
    print("=" * 50)

    # Поиск мероприятий с количеством мест, равным 100
    events_with_100_seats = find_events_with_available_seats(events_collection, 100)
    print(events_with_100_seats)
    print("=" * 50)
