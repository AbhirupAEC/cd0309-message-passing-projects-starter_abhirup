import logging
from typing import Dict, List
import json
from kafka import KafkaProducer

from app import db
from app.udaconnect.models import  Person

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-api")

topic = 'udaconnect_person_create'

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)
 
def on_send_error(excp):
    logger.warning('I am an errback', exc_info=excp)




class PersonService:
    @staticmethod
    def create(person: Dict) -> Person:
        new_person = Person()

        # Create a producer with JSON serializer
        producer = KafkaProducer(
            bootstrap_servers='kafka-broker:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        
        new_person.first_name = person["first_name"]
        new_person.last_name = person["last_name"]
        new_person.company_name = person["company_name"]

        # Sending JSON data
        producer.send(topic, person).add_callback(on_send_success).add_errback(on_send_error)

        #blocked as data write will happen via kafka
        #db.session.add(new_person)
        #db.session.commit()

        return new_person

    @staticmethod
    def retrieve(person_id: int) -> Person:
        person = db.session.query(Person).get(person_id)
        return person

    @staticmethod
    def retrieve_all() -> List[Person]:
        return db.session.query(Person).all()
