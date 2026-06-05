import eventlet
eventlet.monkey_patch()

import json
from cassandra.cluster import Cluster
from cassandra.io.eventletreactor import EventletConnection

base_path = r"C:\Users\jimen\OneDrive\3ºIySD\IBDN\practica_creativa"
file_path = base_path + r"\data\origin_dest_distances.jsonl"

cluster = Cluster(
    ["127.0.0.1"],
    connection_class=EventletConnection
)

session = cluster.connect("agile_data_science")

insert_query = """
INSERT INTO flight_distances (origin, dest, distance)
VALUES (%s, %s, %s)
"""

count = 0

with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue

        row = json.loads(line)

        origin = row.get("Origin") or row.get("origin")
        dest = row.get("Dest") or row.get("dest")
        distance = row.get("Distance") or row.get("distance")

        if origin and dest and distance is not None:
            session.execute(insert_query, (origin, dest, float(distance)))
            count += 1

cluster.shutdown()

print("Distancias insertadas:", count)