import json
import psycopg2
import os
from kafka import KafkaConsumer

# Kafka configuration
TOPIC_NAME = 'udaconnect_person_create'
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
def insert_into_postgres(id,first_name, last_name, company_name):
    conn = create_db_connection()
    if conn is not None:
        cursor = conn.cursor()
        try:
            # Insert record into the table
            cursor.execute(
                "INSERT INTO person (id,first_name, last_name, company_name) VALUES (%s, %s, %s, %s)",
                (id,first_name, last_name, company_name)
            )
            conn.commit()
            print(f"Inserted: {id} {first_name} {last_name} from {company_name}")
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
        person_data = message.value
        id = person_data.get("id")
        first_name = person_data.get("first_name")
        last_name = person_data.get("last_name")
        company_name = person_data.get("company_name")
        
        # Insert the data into PostgreSQL
        insert_into_postgres(id,first_name, last_name, company_name)


if __name__ == "__main__":
    main()
