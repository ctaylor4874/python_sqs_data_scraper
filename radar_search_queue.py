import os
import logging
import time

from helpers import APIHandler, delete_message

import sqs

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
BOTO_QUEUE_NAME_RADAR = 'radar_search_queue'
BOTO_QUEUE_NAME_PLACES = 'google_places_queue'


def get_message(queue):
    message = queue.receive_messages(MaxNumberOfMessages=1)
    if not message:
        return None
    return message[0]


def check_errors(response):
    if response.get('ResponseMetadata', '').get('HTTPStatusCode', '') is not 200:
        logging.info('ERROR! {}'.format(response))
    return response


def send_message(queue, url):
    response = queue.send_message(MessageBody=url)
    check_errors(response)


def make_url(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json?placeid={}&key={}".format(
        place_id, GOOGLE_API_KEY)
    return url


def make_request(queue, message):
    places = APIHandler(message)
    results = places.get_load().get('results', '')
    logging.info("Length of result list: {}".format(len(results)))
    for place in results:
        place_id = place.get('place_id', '')
        if place_id:
            url = make_url(place_id)
            send_message(queue, url)


def run():
    radar_queue = sqs.get_queue(BOTO_QUEUE_NAME_RADAR)
    places_queue = sqs.get_queue(BOTO_QUEUE_NAME_PLACES)
    while True:
        message = get_message(radar_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(30)
            continue
        make_request(places_queue, message.body)
        delete_message(radar_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
