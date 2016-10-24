# -*- coding: utf-8 -*-
'''
Responsible submitting jobs, updating completion status
and throttling.
'''

# Import python libs
from __future__ import absolute_import
from collections import deque
import logging

# Import salt libs
from .run_queue import RunQueue

log = logging.getLogger(__file__)


class SaltJobManager(object):
    '''
    Salt job manager
    '''
    def __init__(self, capacity=100):
        self.run_queue = RunQueue(capacity)

    def submit_one(self, request):
        '''
        Submit an individual request
        '''
        # TODO: What is the best way to do this?
        pass

    def submit_pending(self, pending_requests):
        '''
        Submit pending requests if
        there's space in the queue
        '''
        submitted_requests = []
        while len(pending_requests) > 0 and not self.run_queue.is_full():
            request = pending_requests.popleft()
            log.debug('Submitting request %s', str(request))
            self.submit_one(request)
            self.run_queue.add(request)
            submitted_requests.append(request)
        return submitted_requests

    def poll(self):
        '''
        To be called periodically for reading from input
        queue and submitting jobs
        '''
        log.debug('Salt job manager poll method called')
        pending_requests = deque(__salt__['queue.list_items'](
            __opts__['input_queue']['name'],
            __opts__['input_queue']['backend']
        ))

        to_delete = self.submit_pending(pending_requests)

        if to_delete:
            __salt__['queue.delete'](
                __opts__['input_queue']['name'],
                to_delete,
                __opts__['input_queue']['backend']
            )
