import json
import psycopg2
import os
from kafka import KafkaConsumer
from shapely.geometry import Point
from shapely.wkb import dumps
import binascii

# Kafka configuration
TOPIC_NAME = 'udaconnect_location_create'
KAFKA_BOOTSTRAP_SERVERS = 'kafka-broker:9092'

# PostgreSQL configuration
DB_HOST = os.environ["DB_HOST"]       # Replace with your PostgreSQL host
DB_PORT = os.environ["DB_PORT"]          # Default port for PostgreSQL
DB_NAME = os.environ["DB_NAME"]   # Replace with your database name
DB_USER = os.environ["DB_USERNAME"]      # Replace with your PostgreSQL username
DB_PASSWORD = os.environ["DB_PASSWORD"]  # Replace with your PostgreSQL password

# Create a connection to PostgreSQL
def create_db_connection():
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return connection
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None

# Insert data into PostgreSQL table
def insert_into_postgres(id,person_id,coordinate,creation_time):
    conn = create_db_connection()
    if conn is not None:
        cursor = conn.cursor()
        try:
            # Insert record into the table
            cursor.execute(
                "INSERT INTO location (id,person_id,coordinate,creation_time) VALUES (%s, %s, %s, %s)",
                (id,person_id,coordinate,creation_time)
            )
            conn.commit()
            print(f"Inserted: {id} {person_id} {coordinate} {creation_time}")
        except Exception as e:
            print(f"Error inserting record: {e}")
        finally:
            cursor.close()
            conn.close()

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize the JSON message
)

# Consume messages from the Kafka topic



def main() -> None:
    for message in consumer:
    # Extract the message value
        location_data = message.value
        id = location_data.get("id")
        person_id = location_data.get("person_id")
        creation_time = location_data.get("creation_time")       
        # Create Point with (longitude, latitude)
        point = Point(location_data.get("latitude"),location_data.get("longitude"))
        # Convert to WKB and then to hex string
        wkb_hex = binascii.hexlify(dumps(point, hex=False)).decode("utf-8").upper()
        
        insert_into_postgres(id,person_id,wkb_hex,creation_time)


if __name__ == "__main__":
    main()






# from kafka import KafkaConsumer
# import json

# consumer = KafkaConsumer(
#     'udaconnect_person_create',
#     bootstrap_servers='kafka-broker:9092',
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='my-consumer-group',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# print("Kafka consumer started...")

# for message in consumer:
#     print(f"Received message: {message.value}")















