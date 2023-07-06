from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['electronic_tickets']


def enroll_order(db, order_name, event_name):
    order = db.orders.find_one({"name": order_name})
    event = db.events.find_one({"name": event_name})

    if not order or not event:
        return "Order/event or both doesn't exist."

    if event["available_seats"] == 0:
        return "Unfortunately this event doesn't have available seats."
    elif event["status"] == "finished" or event["status"] == "registration is over":
        return "Registration to this event is over."
    elif event["available_seats"] < order["amount"]:
        return "Not enough available seats."
    else:
        db.events.update_one({'name': event_name}, {"$inc": {"available_seats": -order["amount"]}})
        return "Enrollment successful."


if __name__ == '__main__':
    order_name = "Luke"
    event_name = "Yandex meetup"
    print(enroll_order(db, order_name, event_name))
s