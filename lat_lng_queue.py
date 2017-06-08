import os
import logging
import time
import json

from helpers import delete_message
import sqs

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
BOTO_REGION = 'us-west-2'
BOTO_QUEUE_NAME_RADAR = 'radar_search_queue'
BOTO_QUEUE_NAME_LAT_LNG = 'lat_lng_queue'


def gen_coordinates(start_lat, start_lng, end_lat, end_lng):
    # Cities Scraped: Austin, Houston, Denver, Dallas, SF, Boston, NYC, Seattle,
    # Chicago, LA, SLC, Philly, Raleigh, Atlanta
    # locations = [{
    #     # Atlanta
    #     'start': {
    #         'lat': 33.863164999999974,
    #         'lng': -84.50515199999998
    #     },
    #     'end': {
    #         'lat': 33.872696,
    #         'lng': -84.295349
    #     }
    # }, {
    #     # SLC
    #     'start': {
    #         'lat': 40.495004,
    #         'lng': -112.100372
    #     },
    #     'end': {
    #         'lat': 40.816927,
    #         'lng': -111.770782
    #     }
    # }, {
    #     # Greater Houston
    #     'start': {
    #         'lat': 29.485034,
    #         'lng': -95.910645
    #     },
    #     'end': {
    #         'lat': 30.287532,
    #         'lng': -95.114136
    #     }
    # }, {
    #     # Philly
    #     'start': {
    #         'lat': 39.837014,
    #         'lng': -75.279694
    #     },
    #     'end': {
    #         'lat': 40.151588,
    #         'lng': -74.940491
    #     }
    # }, {
    #     # Raleigh
    #     'start': {
    #         'lat': 35.727284,
    #         'lng': -78.751373
    #     },
    #     'end': {
    #         'lat': 35.827835,
    #         'lng': -78.587265
    #     }
    # }, {
    #     # Atlanta
    #     'start': {
    #         'lat': 33.588311,
    #         'lng': -84.538422
    #     },
    #     'end': {
    #         'lat': 33.872696,
    #         'lng': -84.295349
    #     }
    # }]
    url_list = []
    logging.info("Moved to next city...")
    current_lat = start_lat
    current_lng = start_lng
    while current_lat < end_lat:
        while current_lng < end_lng:
            url_list.append(
                "https://maps.googleapis.com/maps/api/place/radarsearch/json?location={},{}&radius={}&types=restaurant&key={}".format(
                    current_lat, current_lng, 805, GOOGLE_API_KEY
                ))
            current_lng += 0.0083175
        current_lng = start_lng
        current_lat += 0.007233
    return url_list


def check_errors(response):
    if response.get('ResponseMetadata', '').get('HTTPStatusCode', '') is not 200:
        logging.info('ERROR! {}'.format(response))
    return response


def send_message(queue, url):
    response = queue.send_message(MessageBody=url)
    return response


def get_coordinates(queue):
    message = queue.receive_messages(MaxNumberOfMessages=1)
    if not message:
        return None
    return message[0]


def run():
    radar_queue = sqs.get_queue(BOTO_QUEUE_NAME_RADAR)
    lat_lng_queue = sqs.get_queue(BOTO_QUEUE_NAME_LAT_LNG)
    while True:
        message = get_coordinates(lat_lng_queue)
        if not message:
            time.sleep(5)
            continue
        coordinates = json.loads(message.body)
        urls = gen_coordinates(coordinates['start_lat'], coordinates['start_lng'], coordinates['end_lat'], coordinates['end_lng'])
        delete_message(lat_lng_queue, message)
        for url in urls:
            response = send_message(radar_queue, url)
            check_errors(response)


if __name__ == '__main__':
    logging.basicConfig(level=10, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
