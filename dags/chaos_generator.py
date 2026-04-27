import csv
import random

OUTPUT_PATH = "/opt/airflow/dags/data.csv"
NORMAL_HEADERS = ["name", "age", "city"]

ROWS = [
    ["Alice", "30", "Delhi"],
    ["Bob", "25", "Mumbai"],
    ["Carol", "35", "Bangalore"],
]

def generate_data(chaos=False):
    with open(OUTPUT_PATH, "w", newline="") as f:
        if chaos:
            writer = csv.writer(f, delimiter="|")
            writer.writerow(["name", "age", "city", "unknown_column"])
            for row in ROWS:
                writer.writerow(row + ["extra"])
        else:
            writer = csv.writer(f)
            writer.writerow(NORMAL_HEADERS)
            for row in ROWS:
                writer.writerow(row)

    print(f"Data written. Chaos mode: {chaos}")

if __name__ == "__main__":
    chaos = random.choice([True, False])
    generate_data(chaos=chaos)