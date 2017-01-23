# -*- coding: utf-8 -*-
'''
Salt request management
'''

from __future__ import absolute_import

from salt.utils.jid import gen_jid

import logging

STATE_NEW = 'new'

log = logging.getLogger(__name__)


def initialize_request(input_queue, low, queue_reader):
    '''
    Get the request dictionary.
    Clients can call this to submit a request to salt.
    :return The request id
    :rtype str
    '''
    request_id = gen_jid()
    request = {
        'input_queue': input_queue,
        'low': low,
        'jid': None,
        'request_id': request_id,
        'state': STATE_NEW,
    }
    # Add this to the db
    queue_reader.save_request(input_queue, request)
    log.debug('New request initialized')
    return request_id
