import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import json
import os

# Ensure directories exist
os.makedirs("data/raw", exist_ok=True)

fake = Faker()
np.random.seed(42)
random.seed(42)

NUM_ORDERS = 50000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 11, 15)

# Product catalog
products = [
    {"product_id": "PRD001", "name": "Wireless Mouse", "category": "Electronics", "price": 29.99},
    {"product_id": "PRD002", "name": "Laptop Stand", "category": "Electronics", "price": 49.99},
    {"product_id": "PRD003", "name": "Desk Lamp", "category": "Home", "price": 39.99},
    {"product_id": "PRD004", "name": "Office Chair", "category": "Furniture", "price": 299.99},
    {"product_id": "PRD005", "name": "Notebook Set", "category": "Stationery", "price": 12.99},
]

sources = ["Google Ads", "Facebook", "Organic", "Email", "Referral", "Direct"]
states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]

orders = []

for i in range(NUM_ORDERS):
    product = random.choice(products)
    order_date = START_DATE + timedelta(
        days=random.randint(0, (END_DATE - START_DATE).days)
    )

    # Introduce messy fields
    customer_id = None if random.random() < 0.05 else f"CUST{random.randint(1000,9999)}"
    email = fake.email()
    if random.random() < 0.03:
        email = email.replace("@", "_AT_")

    # Date format inconsistency
    if random.random() < 0.04:
        date_str = order_date.strftime("%m/%d/%Y")
    else:
        date_str = order_date.strftime("%Y-%m-%d")

    # Negative amounts
    if random.random() < 0.02:
        amount = -product["price"]
    else:
        amount = product["price"] * random.randint(1, 4)

    # Invalid state
    state = random.choice(states)
    if random.random() < 0.01:
        state = "ZZ"

    # Duplicate order ID
    if random.random() < 0.03 and i > 0:
        order_id = orders[-1]["order_id"]
    else:
        order_id = f"ORD{i+1:07d}"

    # Orphaned product
    if random.random() < 0.02:
        product_id = f"PRD{random.randint(100,999)}"
    else:
        product_id = product["product_id"]

    source = random.choice(sources)
    if random.random() < 0.15:
        source = source.lower()

    orders.append({
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": date_str,
        "product_id": product_id,
        "order_amount": amount,
        "customer_email": email,
        "shipping_state": state,
        "marketing_source": source,
        "order_status": random.choice(["completed", "pending", "cancelled", "refunded"])
    })

df = pd.DataFrame(orders)

# Save outputs
df.to_csv("data/raw/orders.csv", index=False)
with open("data/raw/product_catalog.json", "w") as f:
    json.dump(products, f, indent=2)

print("Generated orders:", len(df))
print("Saved to data/raw/orders.csv")
print("Saved product catalog to data/raw/product_catalog.json")

