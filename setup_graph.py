from neo4j import GraphDatabase

from config import (NEO4J_URI as URI, NEO4J_USER as USER,
                    NEO4J_PASSWORD as PASSWORD, NEO4J_DATABASE as DATABASE)

HOUSE = "house1"
ROOMS = ["kitchen", "living_room", "bedroom"]
SENSORS = ["temperature", "smoke", "gas", "intrusion"]

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD),
                              max_transaction_retry_time=3)
driver.verify_connectivity()
print("Connected to Neo4j")

# 1. Create the house
driver.execute_query(
    "MERGE (h:House {id: $house})",
    house=HOUSE, database_=DATABASE,
)

# 2. Create a central hub that all sensors connect to
driver.execute_query(
    """
    MATCH (h:House {id: $house})
    MERGE (hub:Device {id: $house + '/hub', type: 'hub'})
    MERGE (h)-[:HAS_DEVICE]->(hub)
    """,
    house=HOUSE, database_=DATABASE,
)

# 3. Create the rooms and the sensors inside each room
for room in ROOMS:
    driver.execute_query(
        """
        MATCH (h:House {id: $house})
        MERGE (r:Room {id: $house + '/' + $room, name: $room})
        MERGE (h)-[:HAS_ROOM]->(r)
        """,
        house=HOUSE, room=room, database_=DATABASE,
    )
    for sensor in SENSORS:
        driver.execute_query(
            """
            MATCH (r:Room {id: $house + '/' + $room})
            MATCH (hub:Device {id: $house + '/hub'})
            MERGE (d:Device {id: $house + '/' + $room + '/' + $sensor,
                             type: $sensor})
            MERGE (r)-[:HAS_DEVICE]->(d)
            MERGE (d)-[:CONNECTED_TO]->(hub)
            """,
            house=HOUSE, room=room, sensor=sensor, database_=DATABASE,
        )

print("Graph created")

# 4. Test question: which devices are in the kitchen?
records, _, _ = driver.execute_query(
    """
    MATCH (:Room {name: 'kitchen'})-[:HAS_DEVICE]->(d:Device)
    RETURN d.id AS device
    """,
    database_=DATABASE,
)
print("Devices in the kitchen:")
for record in records:
    print(" -", record["device"])

driver.close()