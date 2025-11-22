import time
import random
import threading
import sys
import os
from confluent_kafka import Producer

# Add the 'generated' folder to path so we can import the protobuf class
sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
import telemetry_pb2

# CONFIGURATION
KAFKA_TOPIC = 'vehicle-telemetry'
KAFKA_CONF = {'bootstrap.servers': 'localhost:9092'}
NUM_CARS = 5  # Number of concurrent threads (cars)

class MockCar:
    def __init__(self, vehicle_id):
        self.vehicle_id = vehicle_id
        # Start near Technopark Tower, Hanoi (approximate)
        self.lat = 20.99 + random.uniform(-0.01, 0.01)
        self.lon = 105.96 + random.uniform(-0.01, 0.01)
        self.speed = 0.0
        self.battery = 100.0
        self.is_autonomous = True

    def update(self):
        """Simulate one time-step of driving"""
        # Battery drain
        self.battery = max(0, self.battery - 0.005)

        # Change speed (accelerate/brake smoothly)
        self.speed = max(0, min(120, self.speed + random.uniform(-5, 5)))

        # Move car based on speed (simplified math)
        # 0.00001 deg is approx 1 meter
        move_factor = (self.speed / 3.6) * 0.00001
        self.lat += random.uniform(-move_factor, move_factor)
        self.lon += random.uniform(-move_factor, move_factor)

        # Random events
        obj_detected = "NONE"
        dist = 0.0

        # 5% chance to detect something
        if random.random() < 0.05:
            obj_detected = random.choice(["PEDESTRIAN", "CAR", "CYCLIST"])
            dist = random.uniform(5.0, 50.0)

        return {
            "lat": self.lat,
            "lon": self.lon,
            "speed": self.speed,
            "battery": self.battery,
            "obj": obj_detected,
            "dist": dist
        }

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f'Message delivery failed: {err}')

def run_car_simulation(vehicle_id, producer):
    car = MockCar(vehicle_id)
    print(f"🚗 Car {vehicle_id} started engine.")

    while True:
        try:
            data = car.update()

            # 1. Create Protobuf Message
            telemetry = telemetry_pb2.VehicleTelemetry()
            telemetry.vehicle_id = vehicle_id
            telemetry.timestamp = int(time.time() * 1000)
            telemetry.latitude = data['lat']
            telemetry.longitude = data['lon']
            telemetry.speed = data['speed']
            telemetry.battery_level = data['battery']
            telemetry.is_autonomous = True
            telemetry.primary_object_detected = data['obj']
            telemetry.object_distance = data['dist']

            # 2. Serialize to Binary
            serialized_data = telemetry.SerializeToString()

            # 3. Send to Kafka
            # We use the vehicle_id as the Key to ensure ordering within partitions
            producer.produce(
                KAFKA_TOPIC,
                key=vehicle_id,
                value=serialized_data,
                on_delivery=delivery_report
            )

            # Trigger the callback to keep the internal queue clean
            producer.poll(0)

            # Simulate frequency (10Hz = 0.1s sleep)
            time.sleep(0.1)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error in car {vehicle_id}: {e}")
            break

if __name__ == "__main__":
    print("Initializing Kafka Producer...")
    producer = Producer(KAFKA_CONF)

    threads = []
    for i in range(1, NUM_CARS + 1):
        v_id = f"VF8-TEST-{i:03d}"
        t = threading.Thread(target=run_car_simulation, args=(v_id, producer))
        t.daemon = True
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping simulation...")
        producer.flush()