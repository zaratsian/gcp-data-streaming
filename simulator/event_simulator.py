
####################################################
#
#   Events Simulator
#
#   This simulator will randomly generate events,
#   at randomized intervals, on user-idefined port.
#
####################################################

import sys
import time
import socket
import random
import json
from random_username.generate import generate_username
from google.cloud import pubsub_v1

####################################################
# Config
####################################################

project_id   = 'dz-apps' 
pubsub_topic = 'data-stream'

usernames = ['mickey', 'minnie', 'snow white', 'anna', 'elsa', 'olaf', 'cinderella', 'belle', 'donald', 'goofy', 'simba']

####################################################
# Functions
####################################################

def pubsub_publish( pubsub_publisher, project_id, pubsub_topic, message ):
    '''
        Pub/Sub Publish Message
        Notes:
          - When using JSON over REST, message data must be base64-encoded
          - Messages must be smaller than 10MB (after decoding)
          - The message payload must not be empty
          - Attributes can also be added to the publisher payload
        
        
        pubsub_publisher  = pubsub_v1.PublisherClient()
        
    '''
    try:
        # Initialize PubSub Path
        pubsub_topic_path = pubsub_publisher.topic_path( project_id, pubsub_topic )
        
        # If message is JSON, then dump to json string
        if type( message ) is dict:
            message = json.dumps( message )
        
        # When you publish a message, the client returns a Future.
        #message_future = pubsub_publisher.publish(pubsub_topic_path, data=message.encode('utf-8'), attribute1='myattr1', anotherattr='myattr2')
        message_future = pubsub_publisher.publish(pubsub_topic_path, data=message.encode('utf-8') )
        message_future.add_done_callback( pubsub_callback )
    except Exception as e:
        print('[ ERROR ] {}'.format(e))


def pubsub_callback( message_future ):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('[ ERROR ] Publishing message on {} threw an Exception {}.'.format(topic_name, message_future.exception()))
    else:
        print('[ INFO ] Result: {}'.format(message_future.result()))


def simulate_payload(enable_sleep=True):
    '''
        score_bias: On a scale of 1-100. It'll bias the scores to be more around that value
    '''
    if enable_sleep:
        time.sleep(random.random()*2)
    
    payload = {
        'name': random.choice(usernames),
        'group': random.choice(['groupA', 'groupB', 'groupC']),
        'duration': random.random() * 1000,
        'flag': random.randint(0,1),
        'score': random.triangular(1,100,30) / 100
    }
    return payload

####################################################
# Main
####################################################

def main():
    
    # PubSub Sink
    try:
        pubsub_publisher = pubsub_v1.PublisherClient()
        while True:
            payload = simulate_payload()
            print('[ INFO ] {}'.format(payload))
            pubsub_publish(pubsub_publisher, project_id=project_id, pubsub_topic=pubsub_topic, message=payload)
    except Exception as e:
        print('[ EXCEPTION ] {}'.format(e))
        sys.exit()


main()


#ZEND
