import logging
from typing import Dict
import json
from app import db
from app.udaconnect.models import Location
from app.udaconnect.schemas import LocationSchema
from kafka import KafkaProducer
from geoalchemy2.functions import ST_AsText, ST_Point
from sqlalchemy.sql import text

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-api")

topic = 'udaconnect_location_create'

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)
 
def on_send_error(excp):
    logger.warning('I am an errback', exc_info=excp)

class LocationService:
    @staticmethod
    def retrieve(location_id) -> Location:
        location, coord_text = (
            db.session.query(Location, Location.coordinate.ST_AsText())
            .filter(Location.id == location_id)
            .one()
        )

        # Rely on database to return text form of point to reduce overhead of conversion in app code
        location.wkt_shape = coord_text
        return location

    @staticmethod
    def create(location: Dict) -> Location:
        # validation_results: Dict = LocationSchema().validate(location)
        # if validation_results:
        #     logger.warning(f"Unexpected data format in payload: {validation_results}")
        #     raise Exception(f"Invalid payload: {validation_results}")

        
        producer = KafkaProducer(
            bootstrap_servers='kafka-broker:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        # new_location = Location()
        # new_location.id = location["id"]
        # new_location.person_id = location["person_id"]
        # new_location.creation_time = location["creation_time"]
        # new_location.coordinate = ST_Point(location["latitude"], location["longitude"])
        


        producer.send(topic, location).add_callback(on_send_success).add_errback(on_send_error)



        return "new location added"

