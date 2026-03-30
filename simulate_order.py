import uuid
from datetime import datetime, timezone
from faker import Faker
import time
import random
from google.cloud import pubsub_v1
import json

PRODUCTS = [
    {"product_id": "SKU-001", "name": "Sneakers Air Max",     "price": 129.99},
    {"product_id": "SKU-002", "name": "T-shirt Premium",      "price": 34.99},
    {"product_id": "SKU-003", "name": "Chaussettes Running",  "price": 8.99},
    {"product_id": "SKU-004", "name": "Short Sport",          "price": 44.99},
    {"product_id": "SKU-005", "name": "Veste Softshell",      "price": 189.99},
]
order = []

PROJECT_ID = "gcp-pipeline-theo"
TOPIC_ID = "orders-raw"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

while True:
    nb_items = random.randint(1,3)
    items = []
    for produit in random.sample(PRODUCTS, nb_items):
      item = produit.copy()        
      item["qty"] = random.randint(1, 3)
      items.append(item)
    items_choisis = random.sample(items,nb_items)


    ts = datetime.now(timezone.utc).isoformat()
    fake = Faker()
    fake.name()       # nom aléatoire
    fake.word()       # mot aléatoire
    commande = {
        "order_id" : f"ORD-{str(uuid.uuid4())[:8].upper()}",
        "timestamp" : ts,
        "user_id" : f"USR-{random.randint(100, 999)}",
        "country" : random.choice(["FR", "DE", "ES", "IT"]),
        "device" : random.choice(["mobile","desktop"]),
        "items" : items_choisis,
        "total_amount" : sum(item["price"] * item["qty"] for item in items)
    }
    data = json.dumps(commande).encode("utf-8")
    future = publisher.publish(topic_path, data)
    future.result()  # attend confirmation
    print(f"[{ts}] {commande['order_id']} — {commande['user_id']} — {commande['country']} — {len(items)} item(s) — {round(commande['total_amount'], 2)}€")
    time.sleep(1)
