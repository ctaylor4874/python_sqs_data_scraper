import os
import logging
import time
from contextlib import closing
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from helpers import APIHandler, Alternator, delete_message, FoursquareSession
from data_parsers.helper_classes import GoogleDetails, FoursquareDetails
import sqs

# Need 3 instances running
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

BOTO_QUEUE_NAME_FS_DETAILS = 'fs_details_queue'
BOTO_QUEUE_NAME_PLACES = 'google_places_queue'

engine = sqlalchemy.create_engine(os.getenv('HAPPYFINDER_ENGINE'), encoding='utf8')
Session = sessionmaker(engine)

INSERT_QUERY = """
       INSERT INTO happyfinder_schema.happyfinder(happyfinder.name, lat, lng, hours,
       rating, phone_number, address, url, google_id, price, fs_venue_id)
       VALUES(:v_name, :lat, :lng, :hours, :rating, :phone_number, :address, 
       :url, :google_id, :price, :fs_venue_id);
       """


def get_message(queue):
    message = queue.receive_messages(MaxNumberOfMessages=1)
    if not message:
        return None
    return message[0]


def check_errors(response):
    if response.get('ResponseMetadata', '').get('HTTPStatusCode', '') is not 200:
        logging.info('ERROR! {}'.format(response))
    return response


def send_message(queue, parsed_data):
    data = {
        'lat': parsed_data.lat,
        'lng': parsed_data.lng,
        'name': parsed_data.name
    }
    response = queue.send_message(MessageBody=json.dumps(data))
    check_errors(response)


def insert_data(data, fs_venue_id):
    if fs_venue_id:
        with closing(Session()) as s:
            try:
                s.execute(INSERT_QUERY, params={
                    'v_name': data.name.encode('utf-8') or None,
                    'lat': data.lat or None,
                    'lng': data.lng or None,
                    'hours': json.dumps(data.hours,
                                        ensure_ascii=False) if data.hours else None,
                    'rating': data.rating if data.rating else None,
                    'phone_number': data.phone_number.encode(
                        'utf-8') if data.phone_number else None,
                    'address': data.address.encode(
                        'utf-8') if data.address else None,
                    'url': data.url.encode('utf-8') if data.url else None,
                    'google_id': data.google_id.encode('utf-8') or None,
                    'price': data.price if data.price else None,
                    'fs_venue_id': fs_venue_id.encode('utf-8') if fs_venue_id else None
                })
            except IntegrityError or Exception as err:
                s.rollback()
                if IntegrityError:
                    logging.info(err)
                else:
                    raise
            else:
                s.commit()


def get_fs_venue_id(parsed_data):
    response = s.get('https://api.foursquare.com/v2/venues/search?intent=match&ll={},{}&query={}'.format(
        parsed_data.lat, parsed_data.lng, parsed_data.name
    ))
    str_response = response.content.decode('utf-8')
    fs_data = FoursquareDetails(json.loads(str_response))
    return fs_data.fs_venue_id


def make_request(queue, message):
    api = APIHandler(message)
    api_data = api.get_load()
    parsed_data = GoogleDetails(api_data)
    fs_venue_id = get_fs_venue_id(parsed_data)
    insert_data(parsed_data, fs_venue_id)
    send_message(queue, parsed_data)


def run():
    credentials_alternator = Alternator()
    google_places_queue = sqs.get_queue(BOTO_QUEUE_NAME_PLACES)
    foursquare_details_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_DETAILS)
    while True:
        message = get_message(google_places_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(5)
            continue
        credentials = credentials_alternator.toggle_foursquare_values()
        make_request(foursquare_details_queue, message.body, credentials)
        delete_message(google_places_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    s = FoursquareSession(version='20170109')
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
