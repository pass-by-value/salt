# -*- coding: utf-8 -*-
'''
Responsible submitting jobs, updating completion status
and throttling.
'''

# Import python libs
from __future__ import absolute_import
from collections import deque
from copy import deepcopy
import logging
import re

# Import salt libs
from salt.runner import RunnerClient
from salt.wheel import WheelClient
from salt.cloud import CloudClient
from salt.utils.jid import gen_jid
from .run_queue import RunQueue
from . import event_utils

STATE_RUNNING = 'running'

STATE_NEW = 'new'

log = logging.getLogger(__name__)


class SaltRequestManager(object):
    '''
    Salt job manager
    '''
    def __init__(self, opts, queue_reader=None):
        '''
        Init data structures and processors
        '''
        self.opts = opts
        self.queues = dict(self._instantiate_queues())
        self.clients = self._instantiate_clients()
        self.queue_reader = queue_reader

        # Create a map for tracking requests
        # queue is a dict
        self.requests = {
            queue['name']: {} for queue in self.opts.get('input_queues', [])
        }

        self.jid_req_map = {}

        self.input_processors = self.init_input_processors()

        self.event_processor = EventProcessor(self)

    def get_req_for_jid(self, jid):
        '''
        Get a request id for this jid if it exists
        None otherwise
        '''
        return self.jid_req_map.get(jid, None)

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
                self) for queue in self.opts.get('input_queues', [])
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
            'runner':   RunnerClient(opts=self.opts),
            'wheel':    WheelClient(opts=self.opts),
            'cloud':    CloudClient(opts=self.opts),
        }

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
        # Add this to the db
        self.save_to_db(input_queue, request)
        # self.input_processors[input_queue].init_request(request)
        log.debug('New request initialized')
        return request_id

    def save_to_db(self, queue, request):
        '''
        Save this to the db
        '''
        self.queue_reader.save_request(queue, request)

    def poll(self):
        '''
        To be called periodically for reading from input
        queue and submitting jobs
        '''
        log.debug('Salt job manager poll method called')

        # Pending requests queued by input_queue name
        pending_requests = self.queue_reader.read_all_queues()

        to_delete = {}
        # Try to submit requests for all queues
        for input_queue in self.opts['input_queues']:
            if input_queue['name'] in pending_requests:
                all_submitted = self.input_processors[
                    input_queue['name']].submit_pending(
                    deque(pending_requests[input_queue['name']]))

                copied = []
                for req_id in all_submitted:
                    req = deepcopy(self.get_request(input_queue['name'], req_id)[0])
                    # so that it gets removed from the queue
                    req['jid'] = None
                    copied.append(req)
                to_delete[input_queue['name']] = copied
        # Delete submitted requests from their input queues
        self.queue_reader.delete_jobs(to_delete)

    def update(self):
        '''
        Check if running jobs have finished
        and remove them from run queue if they have
        '''
        # Process events to detect job finish
        log.debug('Called SaltJobManager update method')

        for req_id, queue, jid in self.event_processor.parse_events():
            log.debug('Completed request %s from queue %s',
                      req_id,
                      queue)
            # Remove this job from the run queue
            self.queues[queue].remove(jid)
            # Remove this job from requests
            self.requests[queue].pop(req_id)
            # Remove this from the jid_req_map
            self.jid_req_map.pop(jid)


class EventProcessor(object):
    '''
    Process events to determine request completion
    '''
    def __init__(self, parent):
        '''
        Init event source this event source
        '''
        self.parent = parent

        self.event_source = event_utils.get_event_source(parent.opts)

        self.ret_re = re.compile('salt/(job|run)/([0-9]{20})/ret')

    def get_pending_events(self):
        '''
        Get the events we haven't seen so far
        '''
        return event_utils.get_pending_events(self.event_source)

    def parse_events(self):
        '''
        Get the request id, queue and jid tuple for all complete events
        '''
        log.debug('Called parse events')
        completed_requests = []
        for event in self.get_pending_events():
            match = re.match(self.ret_re, event['tag'])
            if match:
                jid = match.groups()[1]
                log.debug('Found a match for jid %s', jid)
                if self.parent.get_req_for_jid(jid):
                    req_id, queue = self.parent.get_req_for_jid(jid)
                    log.debug('Adding request %s'
                              ' of queue %s to completed requests',
                              req_id,
                              queue)
                    completed_requests.append((req_id, queue, jid))
        return completed_requests


class InputQueueProcessor(object):
    '''
    Submits jobs from input queue to salt,
    updates the run queue and cleans up the
    input queue
    '''
    def __init__(self, input_queue, parent):
        '''
        Get references to tracking data structures
        '''
        self.requests = parent.requests[input_queue]
        self.input_queue = input_queue
        self.run_queue = parent.queues[input_queue]
        self.clients = parent.clients
        self.jid_req_map = parent.jid_req_map

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
        try:
            return self.clients[request['low']['client'].lower()].async(
                request['low']['fun'], request['low']
            )['jid']
        except Exception as err:
            log.error('Got an error!! %s', err)

    def submit_pending(self, requests):
        '''
        Submit any pending requests if
        there's space in the queue
        :param requests The pending request
        :type requests ``collections.deque``
        :return The requests that we submitted
        :rtype List
        '''
        submitted_requests = []
        log.debug('submitting pending jobs for input queue = %s',
                  self.input_queue)
        while len(requests) > 0 and not self.run_queue.is_full():
            request = deepcopy(requests.popleft())
            log.debug('Submitting request %s', str(request))
            # submit request to salt
            try:
                jid = self._submit_one(request)  # the salt jid
            except Exception:
                log.error('Error submitting request %s', request)
                continue
            # Update the run queue
            self.run_queue.add(jid)

            # Let the callers know which requests we were able
            # to submit
            submitted_requests.append(request['request_id'])

            # Update the request dict used for tracking
            request.update({'jid': jid})
            self.requests.setdefault(
                request['request_id'], []
            ).append(request)
            # update the jid to request_id map
            self.jid_req_map[request['jid']] =\
                request['request_id'], self.input_queue

        return submitted_requests
