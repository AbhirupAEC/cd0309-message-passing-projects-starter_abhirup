FROM python:3.10-alpine

WORKDIR .

COPY kafka_location_data_poller.py .
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "kafka_location_data_poller.py"]