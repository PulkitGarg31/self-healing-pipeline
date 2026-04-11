import csv
import random
import os

OUTPUT_PATH = "/opt/airflow/dags/data.csv"

NORMAL_HEADERS = ["name", "age", "city"]

def generate_data(chaos=False):
    rows = [
        ["Alice", 30, "Delhi"],
        ["Bob", 25, "Mumbai"],
        ["Carol", 35, "Bangalore"],
    ]

    with open(OUTPUT_PATH, "w", newline="") as f:
        if chaos:
            # Schema drift: add unexpected column, change delimiter
            writer = csv.writer(f, delimiter="|")
            writer.writerow(["name", "age", "city", "unknown_column"])
        else:
            writer = csv.writer(f)
            writer.writerow(NORMAL_HEADERS)

        writer.writerows(rows)

    print(f"Data written. Chaos mode: {chaos}")

if __name__ == "__main__":
    chaos = random.choice([True, False])
    generate_data(chaos=chaos)