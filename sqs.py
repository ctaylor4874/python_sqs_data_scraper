"""
Module to maintain consistency across scripts/modules when accessing sqs queues.
"""

import os

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
