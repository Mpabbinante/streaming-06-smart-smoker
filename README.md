# SmartSmoker Temperature Monitoring and Alerting System
Mike Abbinante
22sep23
## Overview

The SmartSmoker Temperature Monitoring and Alerting System is a Python-based project that collects temperature data from a CSV file and sends it to RabbitMQ queues for further processing. It also logs temperature events and triggers alerts based on specific conditions. This system is designed for monitoring temperature in a smoker or similar environment.

## Features

- Reads temperature data from a CSV file.
- Sends temperature data to RabbitMQ queues based on type (e.g., smoker temperature, food A temperature, food B temperature).
- Logs temperature events and alerts to a log file.
- Monitors temperature changes and triggers alerts for specific conditions.
- Option to open the RabbitMQ Admin webpage for queue monitoring.

## Prerequisites

Before setting up and running the project, ensure you have the following prerequisites:

- Python 3.x installed.
- RabbitMQ server installed and running.
- Access to a CSV file with temperature data in the following format:
# Project Tasks
The project will perform the following tasks:

- Establish a connection to the RabbitMQ server.
- Delete any existing queues with specific names (e.g., "01-smoker," "02-food-A," "03-food-B").
- Declare durable queues for each type of temperature data.
- Read data from the CSV file, send it to the appropriate RabbitMQ queue, and log events.
- Check for temperature alerts and log them accordingly.
- output into a log file ("SmartSmoker-temps.log") for detailed logs, including messages sent to queues, temperature alerts, and other events.
# Screenshots
-Running on machine 
![Alt text](<Producer running.PNG>)
-RabbitMQ web
![Alt text](<RabbitMQ web.PNG>)