# -*- coding: utf-8 -*-
'''
    tests.unit.run_queuing.run_queue_test
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
'''

# Import Python libs
from __future__ import absolute_import

# Import Salt Testing libs
from salttesting.unit import TestCase
from salttesting.helpers import ensure_in_syspath

# Import salt libs
from salt.request_queuing.run_queue import RunQueue, QueueFullException

ensure_in_syspath('../../')


class RunQueueTestCase(TestCase):
    '''
    Test salt run queue
    '''
    def test_it_has_a_capacity(self):
        '''
        Make sure that a queue can be initialized with a capacity
        '''
        queue = RunQueue(10)
        self.assertEqual(queue.capacity, 10)

    def test_add(self):
        '''
        Test add path and that
        we cannot add more than capacity
        '''
        queue = RunQueue(1)
        queue.add(1)
        self.assertEqual(1 in queue, True)
        with self.assertRaises(QueueFullException):
            queue.add(2)

        self.assertEqual(2 in queue, False)

    def test_is_full(self):
        '''
        Test functionality to check if queue is full
        '''
        queue = RunQueue(10)
        self.assertEqual(queue.is_full(), False)

        for i in range(10):
            queue.add(i)
        self.assertEqual(queue.is_full(), True)

    def test_it_has_a_name(self):
        '''
        Test functionality to check it supports a name
        '''
        queue = RunQueue(10, 'salt')
        self.assertEqual(queue.name, 'salt')

if __name__ == '__main__':
    from integration import run_tests
    run_tests(RunQueueTestCase, needs_daemon=False)
