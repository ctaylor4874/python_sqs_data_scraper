"""
Script to receive messages from the lat_lng_queue and send a message to the radar_search_queue.
Once a message is received, the message is parsed and processed to prepare the data to send to radar_search_queue.
"""
import os
import logging
import time
import json

import sqs

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
BOTO_QUEUE_NAME_RADAR = 'radar_search_queue'
BOTO_QUEUE_NAME_LAT_LNG = 'lat_lng_queue'


def gen_coordinates(start_lat, start_lng, end_lat, end_lng):
    """
    Generates coordinates to send to the next queue.

    Generates all coordinates between the two coordinates provided and loads creates a url with them.
    The coordinates are then loaded into the url_list to be sent to the radar search queue.  Does this a half mile at
    a time, working west -> east until bounds are hit, then moves north a half mile and repeats until both max lat
     and max lng bounds are hit.
    :param start_lat: Starting latitude for the generator.
    :param start_lng: Starting longitude for the generator.
    :param end_lat: Ending latitude for the generator.
    :param end_lng: Ending longitude for the generator.
    :return: List of url's to send to queue.
    """
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


def run():
    radar_queue = sqs.get_queue(BOTO_QUEUE_NAME_RADAR)
    lat_lng_queue = sqs.get_queue(BOTO_QUEUE_NAME_LAT_LNG)
    while True:
        message = sqs.get_message(lat_lng_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(5)
            continue
        coordinates = json.loads(message.body)
        urls = gen_coordinates(coordinates['start_lat'], coordinates['start_lng'], coordinates['end_lat'],
                               coordinates['end_lng'])
        for url in urls:
            sqs.send_message(radar_queue, url)
        sqs.delete_message(lat_lng_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
