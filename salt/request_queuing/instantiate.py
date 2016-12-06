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
from copy import deepcopy

# Import salt libs
from salt.client import LocalClient
from salt.runner import RunnerClient
from salt.wheel import WheelClient
from salt.cloud import CloudClient
from salt.utils.jid import gen_jid

from .run_queue import RunQueue

STATE_RUNNING = 'running'

STATE_NEW = 'new'

log = logging.getLogger(__name__)


class SaltJobManager(object):
    '''
    Salt job manager
    '''
    def __init__(self, opts):
        self.opts = opts
        self.queues = self._instantiate_queues()
        self.clients = self._instantiate_clients()

    def _instantiate_queues(self):
        '''
        instantiate run queues
        :return: List of tuples (input queue name and
        run queue object)
         :rtype: List(tuple)
        '''
        return [
            (input_queue['name'], RunQueue(input_queue['capacity'],
                                           input_queue['name']))
            for input_queue in self.opts['input_queues']
        ]

    def _instantiate_clients(self):
        '''
        Return a dict of clients
        :return:
        '''
        return {
            'local': LocalClient(mopts=self.opts),
            'runner': RunnerClient(opts=self.opts),
            'wheel': WheelClient(opts=self.opts),
            'cloud': CloudClient(opts=self.opts),
        }

    def _process_new_request(self, request):
        '''
        State transition from new to running
        :return:
        '''
        updated = deepcopy(request)
        queues = dict(self.queues)
        input_queue = request['input_queue']
        if input_queue in queues:
            if not queues[input_queue].is_full():
                updated['jid'] = self._submit_one(request)
                queues[input_queue].add(updated['request_id'])
                updated['state'] = STATE_RUNNING
        return updated

    @staticmethod
    def initialize_request(input_queue, low):
        '''
        Get the request dictionary
        '''
        return {
            'input_queue': input_queue,
            'low': low,
            'jid': None,
            'request_id': gen_jid(),
            'state': STATE_NEW,
        }

    def process_request(self, request):
        input_queue = request['input_queue']
        if input_queue in dict(self.queues):
            self._process_new_request(request)
        else:
            return None

    def _submit_one(self, request):
        '''
        Submit an individual request
        '''
        log.debug('About to submit request to salt ------------------>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        # return self.runner_client.async(request)['jid']
        return self.runner_client.async(
            request,
            {
                'kwarg': {
                    'metadata': {
                        'a': 'b'
                    }
                }
            }
        )['jid']

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
                self._submit_one(request)
            )
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
            log.debug('Removing submitted jobs from input queue')
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
        log.debug('Called SaltJobManager update method')
        if len(self.run_queue) > 0:
            log.debug('There are jobs in the run queue')
            cached_jobs = self.runners['jobs.list_jobs'](
                search_metadata={'foo': 'bar'}
            )
            for job in six.iterkeys(cached_jobs):
                log.debug('Popping job %s off of the run queue', job)
                self.run_queue.remove(job)
