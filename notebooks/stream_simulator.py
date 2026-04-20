#Import necessary libraries
from kafka import KafkaProducer
import json, random, time
from datetime import datetime

# Initialize Kafka producer - make sure Kafka is running and the topic 'retail-pos-stream' is created before executing this code. 
# This producer will send JSON-encoded messages to the specified Kafka topic.
producer = KafkaProducer(                                               # Set the Kafka bootstrap servers (adjust if your Kafka is running on a different host/port)
    bootstrap_servers='localhost:9092',                                 # Configure the producer to serialize Python dictionaries to JSON format before sending them to Kafka - we use a lambda function to convert the dictionary to a JSON string and then encode it as bytes, which is the format Kafka expects for messages.
    value_serializer=lambda v: json.dumps(v).encode('utf-8')            # This means that when we call producer.send() with a Python dictionary, it will automatically be converted to a JSON string and sent as bytes to the Kafka topic. This allows us to easily send structured data (like our sales events) to Kafka and have it be easily consumed by downstream applications that can parse the JSON format.
)

# This is the name of the Kafka topic that we will be sending our simulated sales events to. Make sure this topic exists in your Kafka cluster before running the simulator. You can create it using Kafka command line tools or through a Kafka management interface if you are using a managed Kafka service.
TOPIC = "retail-pos-stream"

# This function generates a simulated sales event with some random data. It creates a dictionary with fields like event_id, store_id, product_id, division, quantity, unit_price, coupon_applied, and event_timestamp. The function also includes some randomness to simulate data quality issues, such as missing event_id, negative quantity, and zero unit_price, which will allow us to test our data quality rules in the next steps.
def generate_event(i):                                                                          # The parameter 'i' is used to create a unique event_id for each generated event. We format it as "EVT" followed by a zero-padded 6-digit number (e.g., EVT000001, EVT000002, etc.). This ensures that each event has a unique identifier, which is important for tracking and processing the events in downstream applications. The use of 'i' allows us to easily generate a sequence of events with unique IDs as we simulate the stream of sales transactions.
    return {                                                                                    # return a dictionary representing a sales event with the following fields:
        "event_id": f"EVT{i:06d}" if random.random() > 0.02 else None,                          # event_id is a unique identifier for the event, formatted as "EVT" followed by a zero-padded 6-digit number. We also introduce a small chance (2%) of it being None to simulate missing data.
        "store_id": f"STORE{random.randint(1, 20):02d}",                                        # store_id is a string representing the store where the transaction took place, formatted as "STORE" followed by a zero-padded 2-digit number (e.g., STORE01, STORE02, etc.).
        "product_id": f"PROD{random.randint(1, 50):03d}",                                       # product_id is a string representing the product being sold, formatted as "PROD" followed by a zero-padded 3-digit number (e.g., PROD001, PROD002, etc.).
        "division": random.choice(['011','012','013','540', None, '999']),                      # division is a string representing the product division, randomly chosen from a list of possible values (including some None values to simulate missing data).
        "quantity": random.randint(1,10) if random.random() > 0.06 else -1,                     # quantity is an integer representing the number of units sold, randomly chosen from 1 to 10. We also introduce a small chance (6%) of it being -1 to simulate negative quantities.
        "unit_price": round(random.uniform(5,300),2) if random.random() > 0.04 else 0,          # unit_price is a float representing the price per unit, randomly chosen from 5 to 300 and rounded to 2 decimal places. We also introduce a small chance (4%) of it being 0 to simulate zero prices.
        "coupon_applied": random.choice([True, False]),                                         # coupon_applied is a boolean indicating whether a coupon was applied to the transaction.
        "event_timestamp": datetime.now().isoformat()                                           # event_timestamp is a string representing the time when the event occurred, formatted as an ISO timestamp.
    }

# print a message to indicate that the Kafka simulator has started, 
# and then enter an infinite loop to continuously generate and send batches of simulated sales events to the Kafka topic.
print("🚀 Kafka simulator started...")
# In each iteration of the loop, we generate a batch of 10 events by calling the generate_event function with an incrementing index 'i' to ensure unique event IDs. We then send each generated event to the Kafka topic using producer.send(). After sending a batch of events, we print a message indicating how many events have been sent so far and then sleep for 5 seconds before generating the next batch. This simulates a continuous stream of sales transactions being sent to Kafka.
i = 0

# Note: This infinite loop will keep running and sending events to Kafka until you manually stop it (e.g., by interrupting the kernel or stopping the script). Make sure to monitor the output and stop it when you are done testing to avoid overwhelming your Kafka cluster with too many messages.
while True:
    for _ in range(10):                                             # Generate and send a batch of 10 events to the Kafka topic
        producer.send(TOPIC, generate_event(i))                     # Call the generate_event function with the current value of 'i' to create a new event, and then send it to the Kafka topic using producer.send(). This will serialize the event as JSON and send it to the specified topic in Kafka.
        i += 1                                                      # Increment 'i' to ensure that the next generated event has a unique event_id. This allows us to keep track of how many events have been generated and sent to Kafka so far.
    print(f"Sent batch up to {i}")                                  # After sending a batch of 10 events, print a message indicating the total number of events sent so far (up to the current value of 'i'). This provides feedback in the console to show that the simulator is actively generating and sending events to Kafka.
    time.sleep(5)                                                   # Sleep for 5 seconds before generating the next batch of events. This simulates a continuous stream of data being sent to Kafka at regular intervals, rather than overwhelming the system with too many events at once. You can adjust the sleep duration as needed to simulate different rates of event generation.