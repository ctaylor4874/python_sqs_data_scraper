"""
Script to receive messages from the radar search queue and send a message to the google places queue.
Once a message is received, the message is parsed and processed to prepare the data to send to google places
queue.
"""
import os
import logging
import time

from helpers import APIHandler

import sqs

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
BOTO_QUEUE_NAME_RADAR = 'radar_search_queue'
BOTO_QUEUE_NAME_PLACES = 'google_places_queue'


def make_url(place_id):
    """
    Generator for the google place details url.

    :param place_id: The google place ID that the url will build on.
    :return: The generated url.
    """
    url = "https://maps.googleapis.com/maps/api/place/details/json?placeid={}&key={}".format(
        place_id, GOOGLE_API_KEY)
    return url


def make_request(queue, message):
    """
    Iterates over the list of places returned from the radar search and builds a URL for each
    place.

    :param queue: The google places SQS container.
    :param message: The message received from the radar search queue.
    :return:
    """
    places = APIHandler(message)
    for place in places.get_load().get('results', ''):
        place_id = place.get('place_id', '')
        if place_id:
            url = make_url(place_id)
            sqs.send_message(queue, url)


def run():
    """
    Runner for radar_search_queue.py.

    If there is no message returned from get_message, the program will sleep for 5 seconds and then
    check again.  Once all requests have been completed, the message is deleted from the queue.
    :return:
    """
    radar_queue = sqs.get_queue(BOTO_QUEUE_NAME_RADAR)
    places_queue = sqs.get_queue(BOTO_QUEUE_NAME_PLACES)
    while True:
        message = sqs.get_message(radar_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(5)
            continue
        make_request(places_queue, message.body)
        sqs.delete_message(radar_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
