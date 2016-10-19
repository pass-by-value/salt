# -*- coding: utf-8 -*-
'''
Implements Salt's run queue.
'''

from __future__ import absolute_import


class QueueFullException(Exception):
    '''
    Exception for run queue
    '''
    pass


class RunQueue(set):
    '''
    Salt's run queue, backed by a set
    '''

    def __init__(self, capacity, name='run_queue'):
        '''
        :param capacity: The capacity of this queue
        :type int
        '''
        set.__init__(self)
        self.capacity = capacity
        self.name = name

    def add(self, item):
        '''
        Adds this item to the capacity.
        Throws `QueueFullException` if
        trying to add more than capacity.
        :param item: The item to add
        :return: None
        '''
        if len(self) > self.capacity - 1:
            raise QueueFullException()
        else:
            super(RunQueue, self).add(item)

    def is_full(self):
        '''
        Returns True if the queue is full
        and False otherwise
        :rtype bool
        '''
        return len(self) == self.capacity
