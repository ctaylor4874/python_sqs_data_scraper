import os
import logging
import time
from contextlib import closing
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from helpers import APIHandler, Alternator, delete_message
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


def make_url(parsed_data, credentials):
    url = "https://api.foursquare.com/v2/venues/search?intent=match&ll={},{}&query={}&client_id={}&client_secret={}&v=20170109".format(
        str(parsed_data.lat), str(parsed_data.lng), parsed_data.name, credentials.foursquare_client_id,
        credentials.foursquare_client_secret)
    return url


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


def get_fs_venue_id(message):
    fs_api = APIHandler(message)
    fs_api_data = fs_api.get_load()
    fs_data = FoursquareDetails(fs_api_data)
    return fs_data.fs_venue_id


def make_request(queue, message, credentials):
    api = APIHandler(message)
    api_data = api.get_load()
    parsed_data = GoogleDetails(api_data)
    url = make_url(parsed_data, credentials)
    fs_venue_id = get_fs_venue_id(url)
    insert_data(parsed_data, fs_venue_id)
    sqs.send_message(queue, url)


def run():
    credentials_alternator = Alternator()
    google_places_queue = sqs.get_queue(BOTO_QUEUE_NAME_PLACES)
    foursquare_details_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_DETAILS)
    while True:
        message = sqs.get_message(google_places_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(5)
            continue
        credentials = credentials_alternator.toggle_foursquare_values()
        make_request(foursquare_details_queue, message.body, credentials)
        sqs.delete_message(google_places_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
