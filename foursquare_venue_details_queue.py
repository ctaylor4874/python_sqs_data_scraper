import os
import logging
import time
import contextlib
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from helpers import APIHandler
from data_parsers.helper_classes import GoogleDetails, FoursquareDetails
import sqs

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
FOURSQUARE_CLIENT_ID = os.getenv('FOURSQUARE_CLIENT_ID')
FOURSQUARE_CLIENT_SECRET = os.getenv('FOURSQUARE_CLIENT_SECRET')
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


def send_message(queue, url):
    response = queue.send_message(MessageBody=url)
    check_errors(response)


def make_url(parsed_data):
    url = "https://api.foursquare.com/v2/venues/search?intent=match&ll={},{}&query={}&client_id={}&client_secret={}&v=20170109".format(
        str(parsed_data.lat), str(parsed_data.lng), parsed_data.name, FOURSQUARE_CLIENT_ID, FOURSQUARE_CLIENT_SECRET)
    return url


def insert_data(data, fs_venue_id):
    with contextlib.closing(Session()) as s:
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
        except IntegrityError or Exception as e:
            s.rollback()
            if IntegrityError:
                logging.info(e)
                pass
            else:
                raise
        else:
            s.commit()


def make_request(queue, message):
    api_data = APIHandler(message)
    parsed_data = GoogleDetails(api_data)
    url = make_url(parsed_data)
    fs_api_data = APIHandler(message)
    fs_data = FoursquareDetails(fs_api_data)
    fs_venue = FoursquareDetails(fs_data)
    insert_data(parsed_data, fs_venue.fs_venue_id)
    send_message(queue, url)


def delete_message(queue, message):
    """
    Delete the message from the queue.

    See: http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Queue.delete_messages
    :return:
    """
    entries = [
        {
            'Id': message.message_id,
            'ReceiptHandle': message.receipt_handle
        }
    ]
    response = queue.delete_messages(Entries=entries)

    successful = response.get('Successful', [{}])[0].get('Id') == message.message_id
    if successful:
        return

    failure_details = response.get('Failed', [{}])
    failed_message_id = failure_details.get('Id')
    if failed_message_id != message.message_id:
        raise ValueError('Delete message was unsuccessful but failed message id does not match expected message id. '
                         'failed_message_id={} expected_message_id={}'.format(failed_message_id, message.message_id))

    raise ('Details: {}\nID: {}'.format(failure_details, failed_message_id))


def run():
    google_places_queue = sqs.get_queue(BOTO_QUEUE_NAME_PLACES)
    foursquare_details_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_DETAILS)
    while True:
        message = get_message(google_places_queue)
        if not message:
            time.sleep(5)
            continue
        make_request(foursquare_details_queue, message.body)
        delete_message(google_places_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=10, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
