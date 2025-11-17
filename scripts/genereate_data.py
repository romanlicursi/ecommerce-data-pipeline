import pandas as pd
import numpy as np
from faker import Faker
fake = Faker()

np.random.seed(42)

row_count = 50000

orders = []
for i in range(row_count):
    orders.append({
        "order_id": i + 1,
        "customer_id": fake.random_int(min=1, max=8000),
        "order_date": fake.date_between(start_date="-2y", end_date="today"),
        "state": fake.random_element(["MN","CA","NY","TX","FL","WA","ZZ","None",""]),
        "quantity": fake.random_int(min=1, max=10),
        "unit_price": round(np.random.uniform(5,500), 2)
    })

df = pd.DataFrame(orders)

df.to_csv("data/raw/orders_raw.csv", index=False)
print("Generated raw dataset with 50000 rows.")

