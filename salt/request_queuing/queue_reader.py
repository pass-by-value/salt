# -*- coding: utf-8 -*-
'''

'''

# Import python libs
from __future__ import absolute_import

import six


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
            pending[queue['name']] = self.runner_funcs['queue.list_items'](
                queue['name']
            )[queue['capacity']]
        return pending
