# -*- coding: utf-8 -*-
'''
Responsible submitting jobs, updating completion status
and throttling.
'''

# Import python libs
from __future__ import absolute_import
from collections import deque
import logging
import six

# Import salt libs
from .run_queue import RunQueue

log = logging.getLogger(__file__)


class SaltJobManager(object):
    '''
    Salt job manager
    '''
    def __init__(self,
                 capacity=100,
                 runner_client=None,
                 opts=None,
                 runners=None):
        self.run_queue = RunQueue(capacity)
        # TODO: Add ability to handle all Salt clients
        self.runner_client = runner_client
        self.opts = opts
        self.runners = runners

    def submit_one(self, request):
        '''
        Submit an individual request
        '''
        return self.runner_client.async(request['low'])['jid']

    def submit_pending(self, pending_requests):
        '''
        Submit any pending requests if
        there's space in the queue
        '''
        submitted_requests = []
        while len(pending_requests) > 0 and not self.run_queue.is_full():
            request = pending_requests.popleft()
            log.debug('Submitting request %s', str(request))
            self.run_queue.add(
                self.submit_one(request))
            submitted_requests.append(request)
        return submitted_requests

    def poll(self):
        '''
        To be called periodically for reading from input
        queue and submitting jobs
        '''
        log.debug('Salt job manager poll method called')
        pending_requests = deque(self.runners['queue.list_items'](
            self.opts['input_queue']['name'],
            self.opts['input_queue']['backend']
        ))

        to_delete = self.submit_pending(pending_requests)

        if to_delete:
            self.runners['queue.delete'](
                self.opts['input_queue']['name'],
                to_delete,
                self.opts['input_queue']['backend']
            )

    def update(self):
        '''
        Check if running jobs have finished
        and remove them from run queue if they have
        '''
        if len(self.run_queue) > 0:
            cached_jobs = self.runners['jobs.list_jobs'](
                search_metadata=self.opts['metadata']
            )
            for job in six.iterkeys(cached_jobs):
                self.run_queue.remove(job)
