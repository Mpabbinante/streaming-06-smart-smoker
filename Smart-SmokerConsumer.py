"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

  Author: Mike Abbinante
    Date: 29Sep23

"""
import pika
import sys
import logging
from collections import deque
from dateutil import parser

# Specify the log file path
LOG_FILE = "SmartSmoker-Consumer.log"

# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w"),  # Log to a file
        logging.StreamHandler()  # Show message stream on the console
    ]
)

# Define variables for temperature monitoring
SMOKER_TEMPERATURES = deque(maxlen=5)  # Max length for the smoker time window
FOOD_TEMPERATURES = deque(maxlen=20)   # Max length for the food time window

SMOKER_ALERT_TEMPTHRESHOLD = 15.0  # Smoker temperature decrease threshold (in degrees F)
FOOD_STALL_TEMPTHRESHOLD = 1.0     # Food temperature change threshold (in degrees F)

def smoker_callback(ch, method, properties, body):
    '''
    Create callback and send message for smoker temperatures
    '''
    queue_name = "01-smoker"  # Define the queue_name here
    process_temperature(body, "Smoker", SMOKER_TEMPERATURES, SMOKER_ALERT_TEMPTHRESHOLD, ch, queue_name)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_a_callback(ch, method, properties, body):
    '''
    Create callback and send message for Food A temperatures
    '''
    queue_name = "02-FoodA"  # Define the queue_name here
    process_temperature(body, "Food A", FOOD_TEMPERATURES, FOOD_STALL_TEMPTHRESHOLD,ch, queue_name)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_b_callback(ch, method, properties, body):
    '''
    Create callback and send message for Food B temperatures
    '''
    queue_name = "03-Foodb"  # Define the queue_name here
    process_temperature(body, "Food B", FOOD_TEMPERATURES, FOOD_STALL_TEMPTHRESHOLD,ch, queue_name)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def process_temperature(body, temperature_name, temperature_deque, alert_threshold, channel, queue_name):
    try:
        parts = body.decode().split(',')

        if len(parts) < 2:
            logging.warning(f"Invalid message format for {temperature_name} callback: {body}")
            return

        timestamp_str, smoker_temp_str = parts[:2]
        try:
            # Use dateutil.parser to automatically parse the timestamp
            timestamp = parser.parse(timestamp_str)
        except ValueError:
            logging.warning(f"Unable to parse timestamp '{timestamp_str}' for {temperature_name} callback.")
            return

        smoker_temp = float(smoker_temp_str.strip()) if smoker_temp_str.strip().lower() != 'none' else None

        temperature_deque.append((timestamp, smoker_temp))

        if len(temperature_deque) == temperature_deque.maxlen:
            valid_temperatures = [temp[1] for temp in temperature_deque if temp[1] is not None]

            if len(valid_temperatures) < 2:
                logging.warning(f"Invalid temperatures amount for {temperature_name} callback.")
                return

            first_temp = valid_temperatures[0]
            last_temp = valid_temperatures[-1]
            time_difference = round(
                (temperature_deque[-1][0] - temperature_deque[0][0]).total_seconds() / 60)

            # Log the message instead of sending it to the queue
            logging.info(f"Received from {queue_name} Queue: Timestamp={timestamp}, Temperature={smoker_temp}")

            if abs(first_temp - last_temp) >= alert_threshold and time_difference <= get_time_window(
                    temperature_name):
                logging.info(
                    f"{temperature_name} Alert! Temperature change >= {alert_threshold}°F in {time_difference} minutes.")

    except Exception as e:
        logging.error(f"Error in {temperature_name} callback: {e}")

def get_time_window(temperature_name):
    '''
    Return the amount of time within which a drastic temperature change needs to occur to trigger logging of the event.

    Parameters:
        temperature_name
    '''
    if temperature_name == "Smoker":
        return 2.5  # Smoker time window is 2.5 minutes
    elif temperature_name == "Food A":
        return 10.0   # Food time window is 10 minutes
    elif temperature_name == "Food B":
        return 10.0   # Food time window is 10 minutes
    else:
        return None

# Define a main function to run the program
def main(hn: str = "localhost"):
    """ Continuously listen for task messages on multiple named queues."""

    # Create a list of queues and their associated callback functions
    queues_and_callbacks = [
        ("01-smoker", smoker_callback),
        ("02-food-A", food_a_callback),
        ("03-food-B", food_b_callback)
    ]

    try:
        # Create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

        # Use the connection to create a communication channel
        channel = connection.channel()

        for queue_name, callback_function in queues_and_callbacks:
            # Use the channel to declare a durable queue
            # A durable queue will survive a RabbitMQ server restart
            # And help ensure messages are processed in order
            # Messages will not be deleted until the consumer acknowledges
            channel.queue_declare(queue=queue_name, durable=True)

            # The QoS level controls the # of messages
            # That can be in-flight (unacknowledged by the consumer)
            # At any given time.
            # Set the prefetch count to one to limit the number of messages
            # Being consumed and processed concurrently.
            # This helps prevent a worker from becoming overwhelmed
            # And improve the overall system performance.
            # prefetch_count = Per consumer limit of unacknowledged messages
            channel.basic_qos(prefetch_count=1)

            # Configure the channel to listen on a specific queue,
            # Use the appropriate callback function,
            # And do not auto-acknowledge the message (let the callback handle it)
            channel.basic_consume(queue=queue_name, on_message_callback=callback_function, auto_ack=False)

        # Log a message for the user
        logging.info(" [*] Ready for work. To exit press CTRL+C")

        # Start consuming messages via the communication channel
        channel.start_consuming()

    # Except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logging.info("")
        logging.error("ERROR: Something went wrong.")
        logging.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("")
        logging.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logging.info("\nClosing connection. Goodbye.\n")
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# Without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # Call the main function with the information needed
    main("localhost")