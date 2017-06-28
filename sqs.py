"""
Module to maintain consistency across scripts/modules when accessing SQS queues.
"""

import os
import logging
import boto3

BOTO_REGION = 'us-west-2'

AWS_ACCESS_KEY = os.getenv('PERSONAL_AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('PERSONAL_AWS_SECRET_KEY')

sqs = boto3.resource('sqs', region_name=BOTO_REGION, aws_access_key_id=AWS_ACCESS_KEY,
                     aws_secret_access_key=AWS_SECRET_KEY)


def get_queue(queue_name):
    """
    Get an aws SQS object by name.

    See: http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#queue

    :param queue_name:
    :return:
    """
    return sqs.get_queue_by_name(QueueName=queue_name)


def get_message(queue):
    """
    Makes a call to the google_places_queue to see if any messages is waiting to dispatch.

    :param queue: The queue to receive a message from.
    :return: Message received from the queue, or none.
    """
    message = queue.receive_messages(MaxNumberOfMessages=1)
    if not message:
        return None
    return message[0]


def check_errors(response):
    """
    Checks for an error response from SQS after sending a message.

    :param response: The response from SQS.
    :return: The response after checking and logging Errors.
    """
    if response.get('ResponseMetadata', '').get('HTTPStatusCode', '') is not 200:
        logging.info('ERROR! {}'.format(response))
    return response


def send_message(queue, data):
    """
    Sends a message up to the specified SQS Queue.

    :param queue: The queue to send the message to.
    :param data: The data to be sent to the queue.
    :return:
    """
    response = queue.send_message(MessageBody=data)
    check_errors(response)


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


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
