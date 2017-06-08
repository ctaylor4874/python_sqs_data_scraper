import os
import logging
import time

from helpers import APIHandler

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
    for place in places.get_load().get('results', ''):
        place_id = place.get('place_id', '')
        if place_id:
            url = make_url(place_id)
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
    radar_queue = sqs.get_queue(BOTO_QUEUE_NAME_RADAR)
    places_queue = sqs.get_queue(BOTO_QUEUE_NAME_PLACES)
    while True:
        message = get_message(radar_queue)
        if not message:
            time.sleep(5)
            continue
        make_request(places_queue, message.body)
        delete_message(radar_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=10, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
