import json
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Fixed product catalogue — keeps data consistent across runs
PRODUCTS = [
    {"id": "P001", "name": "Wireless Headphones",   "price": 79.99,  "category": "Electronics"},
    {"id": "P002", "name": "Laptop Stand",           "price": 34.99,  "category": "Accessories"},
    {"id": "P003", "name": "USB-C Hub",              "price": 49.99,  "category": "Electronics"},
    {"id": "P004", "name": "Mechanical Keyboard",    "price": 129.99, "category": "Electronics"},
    {"id": "P005", "name": "Webcam HD",              "price": 59.99,  "category": "Electronics"},
    {"id": "P006", "name": "Desk Lamp LED",          "price": 29.99,  "category": "Home Office"},
    {"id": "P007", "name": "Ergonomic Mouse",        "price": 44.99,  "category": "Accessories"},
    {"id": "P008", "name": "Monitor Arm",            "price": 89.99,  "category": "Home Office"},
    {"id": "P009", "name": "Cable Management Kit",   "price": 19.99,  "category": "Accessories"},
    {"id": "P010", "name": "Noise Cancelling Mic",   "price": 99.99,  "category": "Electronics"},
]

# Event types with realistic weights
# Most visitors just view pages, fewer add to cart, even fewer buy
EVENT_TYPES   = ["page_view", "add_to_cart", "purchase", "return"]
EVENT_WEIGHTS = [55, 25, 15, 5]

COUNTRIES = ["SE", "DE", "US", "GB", "FR", "NL", "NO", "DK", "FI", "PL"]
DEVICES   = ["mobile", "desktop", "tablet"]


def generate_events(n: int = 500) -> list:
    """
    Generate n fake e-commerce events.
    Each event represents one user action on the platform.
    """
    events = []
    now = datetime.utcnow()

    for _ in range(n):
        product    = random.choice(PRODUCTS)
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
        quantity   = random.randint(1, 4) if event_type in ["purchase", "add_to_cart"] else 0

        # Revenue only applies to purchases
        revenue = round(product["price"] * quantity, 2) if event_type == "purchase" else 0.0

        # Spread events across the last 24 hours
        event_time = now - timedelta(minutes=random.randint(0, 1440))

        events.append({
            "event_id":       fake.uuid4(),
            "event_type":     event_type,
            "user_id":        fake.uuid4(),
            "session_id":     fake.uuid4(),
            "product_id":     product["id"],
            "product_name":   product["name"],
            "category":       product["category"],
            "unit_price":     product["price"],
            "quantity":       quantity,
            "revenue":        revenue,
            "country":        random.choice(COUNTRIES),
            "device_type":    random.choice(DEVICES),
            "created_at":     event_time.isoformat(),
        })

    return events


if __name__ == "__main__":
    """
    To test it directly run:
    python include/generate_events.py
    """
    events = generate_events(10)
    for e in events:
        print(json.dumps(e, indent=2))
    print(f"\nGenerated {len(events)} events successfully")