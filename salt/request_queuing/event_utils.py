# -*- coding: utf-8 -*-
'''
Get the event source
'''

# Import python libs
from __future__ import absolute_import
import logging

# Import salt libs
from salt.utils.event import get_event

log = logging.getLogger(__name__)


def get_event_source(opts):
    '''
    Return the master event source
    '''
    log.debug('Returning event source')
    return get_event(
        'master',
        sock_dir=opts['sock_dir'],
        transport=opts['transport'],
        opts=opts,
        listen=True
    )


def get_pending_events(event_source):
    '''
    :param event_source: The ``MasterEvent`` object
    :return: List of events
    '''
    pending = []

    curr = event_source.get_event(wait=0.001, full=True)
    while curr:
        pending.append(curr)
        curr = event_source.get_event(wait=0.001, full=True)

    log.debug('Returning pending events')
    return pending
