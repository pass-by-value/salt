# -*- coding: utf-8 -*-
'''
Read and write to the db backed input queue
'''

# Import python libs
from __future__ import absolute_import
import logging

import six


log = logging.getLogger(__name__)

BACKEND = 'pgjsonb'


class QueueReader(object):
    def __init__(self, queues, runner_funcs):
        '''
        Store instances
        '''
        self.queues = queues
        self.runner_funcs = runner_funcs

    def read_all_queues(self):
        '''
        Returns the data structures
        associated with the input queue.
        '''
        pending = {}
        for queue in self.queues:
            log.debug('Reading data for queue %s', queue)
            reqs = self.runner_funcs['queue.list_items'](
                queue['name'],
                backend=BACKEND
            )
            if reqs:
                pending[queue['name']] = reqs[0:queue['capacity']]
        return pending

    def delete_jobs(self, to_delete):
        '''
        For each queue delete the given jobs
        :param to_delete: List of dicts keyed by queue
        '''
        for queue in six.iterkeys(to_delete):
            log.debug('Deleting all '
                      'submitted requests for queue %s', queue)
            self.runner_funcs['queue.delete'](
                queue,
                to_delete[queue],
                backend=BACKEND)
            log.debug('Deleted all '
                      'submitted requests for queue %s', queue)

    def save_request(self, queue, request):
        '''
        Save this request to the db
        '''
        log.debug('Saving request to db')
        self.runner_funcs['queue.insert'](
            queue,
            [request],
            backend=BACKEND
        )
        log.debug('Saved request to db')
