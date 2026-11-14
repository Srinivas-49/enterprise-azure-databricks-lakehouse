from src.lakehouse import run
import json

print(json.dumps(run("data/orders.csv", "outputs"), indent=2))
