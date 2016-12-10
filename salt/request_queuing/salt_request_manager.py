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

'''
[{
    'tag': '20161208114705304086',
    'data': {
        '_stamp': '2016-12-08T16:47:05.305210',
        'minions': ['abstraction.local']
    }
}, {
    'tag': 'salt/job/20161208114705304086/new',
    'data': {
        'tgt_type': 'glob',
        'jid': '20161208114705304086',
        'tgt': '*.local',
        '_stamp': '2016-12-08T16:47:05.305601',
        'user': 'sudo_adi',
        'arg': [],
        'fun': 'test.ping',
        'minions': ['abstraction.local']
    }
}, {
    'tag': 'salt/job/20161208114705304086/ret/abstraction.local',
    'data': {
        'fun_args': [],
        'jid': '20161208114705304086',
        'return': True,
        'retcode': 0,
        'success': True,
        'cmd': '_return',
        '_stamp': '2016-12-08T16:47:05.374480',
        'fun': 'test.ping',
        'id': 'abstraction.local',
        'metadata': {
            'foo': 'bar'
        }
    }
}]
'''


class SaltRequestManager(object):
    '''
    Salt job manager
    '''
    def __init__(self, opts, queue_reader=None):
        self.opts = opts
        self.queues = self._instantiate_queues()
        self.clients = self._instantiate_clients()
        self.queue_reader = queue_reader

        # Create a map for tracking requests
        # queue is a dict
        self.requests = {
            queue['name']: {} for queue in self.opts.get('input_queues', [])
        }

        self.input_processors = self.init_input_processors()

    def get_request(self, input_queue, request_id):
        '''
        Get stored requests
        '''
        return self.requests.get(
            input_queue,
            {}
        ).get(request_id, None)

    def init_input_processors(self):
        '''
        Store an input processor for
        each input queue
        :return:
        '''
        return {
            queue['name']: InputQueueProcessor(
                queue['name'],
                dict(self.queues),
                self.requests[queue['name']]
            ) for queue in self.opts.get('input_queues', [])
        }

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
            for input_queue in self.opts.get('input_queues', [])
        ]

    def _instantiate_clients(self):
        '''
        Return a dict of clients
        :return:
        '''
        return {
            'local':    LocalClient(),
            'runner':   RunnerClient(opts=self.opts),
            'wheel':    WheelClient(opts=self.opts),
            'cloud':    CloudClient(opts=self.opts),
        }

    # re.match('salt/job/[0-9]{20}/ret', 'salt/job/20161208114705304086/new')
    # x = re.match('salt/job/([0-9]{20})/ret', 'salt/job/20161208114705304086/ret/foo.bar')

    def initialize_request(self, input_queue, low):
        '''
        Get the request dictionary
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
        log.debug('New request initialized')
        self.input_processors[input_queue].init_request(request)
        return request_id

    def poll(self):
        '''
        To be called periodically for reading from input
        queue and submitting jobs
        '''
        log.debug('Salt job manager poll method called')

        pending_requests = get_from_service()
        to_delete = {}
        # Try to submit requests for all queues
        for input_queue in self.opts['input_queues']:
            to_delete[input_queue['name']] = \
                self.input_processors[input_queue['name']].submit_pending(
                pending_requests[input_queue['name']])

        # Delete submitted requests from their input queues
        for input_queue in self.opts['input_queues']:
            if to_delete[input_queue['name']]:
                log.debug('Removing submitted jobs from input queue')
                self.runners['queue.delete'](
                    input_queue['name'],
                    to_delete,
                    input_queue['backend']
                )

    def update(self):
        '''
        Check if running jobs have finished
        and remove them from run queue if they have
        '''
        # Process events to detect job finish
        log.debug('Called SaltJobManager update method')
        if len(self.run_queue) > 0:
            log.debug('There are jobs in the run queue')
            cached_jobs = self.runners['jobs.list_jobs'](
                search_metadata={'foo': 'bar'}
            )
            for job in six.iterkeys(cached_jobs):
                log.debug('Popping job %s off of the run queue', job)
                self.run_queue.remove(job)


class InputQueueProcessor(object):
    def __init__(self, input_queue, queues, requests):
        self.requests = requests
        self.input_queue = input_queue
        self.run_queue = queues[input_queue]

    def init_request(self, request):
        '''
        :param request: Salt's low data
        :return: Dictionary for request tracking
        '''
        self.requests.setdefault(
            request['request_id'], []).append(request)

    def _submit_one(self, request):
        '''
        Submit an individual request
        calls the async method on the client
        '''
        log.debug('About to submit request to salt')
        return self.clients[request['low']['client'].lower()].async(
            request, {}
        )['jid']

    def submit_pending(self, requests):
        '''
        Submit any pending requests if
        there's space in the queue
        :return The requests that we submitted
        :rtype List
        '''
        submitted_requests = []
        while len(requests) > 0 and not self.run_queue.is_full():
            request = deepcopy(requests.popleft())
            log.debug('Submitting request %s', str(request))
            # submit request to salt
            jid = self._submit_one(request)  # the salt jid

            # Update the run queue
            self.run_queue.add(jid)

            # Update the request dict used for tracking
            request.update({'jid': jid})
            self.requests.setdefault(
                request['request_id'], []
            ).append(request)

            # Let the callers know which requests we were able
            # to submit
            submitted_requests.append(request)

        return submitted_requests
