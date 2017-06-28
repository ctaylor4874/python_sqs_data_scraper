import os
import logging
import time
import json

import sqs

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
BOTO_QUEUE_NAME_RADAR = 'radar_search_queue'
BOTO_QUEUE_NAME_LAT_LNG = 'lat_lng_queue'


def gen_coordinates(start_lat, start_lng, end_lat, end_lng):
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
