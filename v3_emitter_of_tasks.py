"""
Course: 44-671 : Module 04
Student: Erin Swan-Siegel
Date: 09-09-2023

Additions to this code as compared with version 2 include
    1. a True / False variable for prompting the user for the use of the queue monitoring
    2. a True / False variable for prompting the user whether they'd like to read in data from a file
    3. Prompting the user to indicate whether their file has a header or not
    4. Prompting the user for a file name
    5. Using the user input to access their desired input file and read or skip the first record of the file

    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Denise Case
    Date: January 15, 2023

"""

import pika
import sys
import webbrowser
import csv
import socket
import time
import logging

# Function to prompt user to use the RabbitMQ Admin website; runs when show_offer == True
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

# Function to prompt user for csv input file information; runs when input_yn == True
def input_file_name():
    """Offer to use an input .csv file"""
    ans = input("Would you like to import messages from a .csv file? y or n ")
    print()
    if ans.lower() == "y":
        user_header = input("Does the file contain a header row? y or n ?")
        print()
        user_file = input("Please input file name [without .csv extension] : ")
        print()
        user_file = user_file + ".csv"
    return user_header, user_file
    
# Function remains unchanged from v2 
def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Define Streaming data process
# Based on streaming code from Module 01
# Function accepts user information regarding the file to be read
def stream_row(file_name, header_status):
    """Read from input file and stream data."""
    logging.info(f"Starting to stream data from {file_name}")

    # Create a file object for input (r = read access)
    with open(file_name, "r") as input_file:
        logging.info(f"Opened for reading: {file_name}.")

        # Create a CSV reader object
        reader = csv.reader(input_file, delimiter=",")
        
        # If the user indicated a header is present, skip the header row and continue
        if header_status == 'y':
            header = next(reader)  # Skip header row
            logging.info(f"Skipped header row: {header}")

        # For each data row in the reader, read and send as a message then pause before reading the next row
        for row in reader:
            print(row)
            send_message("localhost","task_queue2",row[0])
            logging.info(f"Reading row {reader.line_num}.")
            time.sleep(3) # wait 3 seconds between messages


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__": 
    # ask the user if they'd like to open the RabbitMQ Admin site
    # based on the T/F setting of show_offer variable
    show_offer = True 
    if show_offer == True:
        offer_rabbitmq_admin_site()
    
    # ask the user if they'd like to get messages from a .csv file
    # based on the T/F setting of input_yn variable
    # Returns arguments from input function to run stream function
    input_yn = True
    if input_yn == True:
        user_header, user_file = input_file_name()
        stream_row(user_file, user_header)
