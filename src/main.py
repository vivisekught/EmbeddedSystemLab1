from paho.mqtt import client as mqtt_client
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from file_datasource import FileDatasource
import config
from schema.parking_shema import ParkingSchema


def connect_mqtt(broker, port):
    """Create MQTT client"""
    print(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print("Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client


def publish(client, datasource, delay):
    datasource.startReading()

    while True:
        time.sleep(delay)
        # Publish accelerometer data
        accelerometer_data = datasource.read_accelerometer()
        msg_accelerometer = AggregatedDataSchema().dumps(accelerometer_data)
        client.publish(config.MQTT_TOPIC_ACCELEROMETER, msg_accelerometer)

        # Publish parking data
        parking_data = datasource.read_parking()
        msg_parking = ParkingSchema().dumps(parking_data)
        client.publish(config.MQTT_TOPIC_PARKING, msg_parking)


def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")
    # Infinity publish data
    publish(client, datasource, config.DELAY)


if __name__ == '__main__':
    run()
