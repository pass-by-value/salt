# -*- coding: utf-8 -*-
'''
    tests.unit.run_queuing.salt_job_manager_test
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
'''

# Import Python libs
from __future__ import absolute_import
from collections import deque

# Import Salt Testing libs
from salttesting.unit import TestCase
from salttesting.helpers import ensure_in_syspath
from salttesting.mock import MagicMock

# Import salt libs
import salt.request_queuing.salt_job_manager
from salt.request_queuing.salt_job_manager import SaltJobManager
# from salt.request_queuing.run_queue import RunQueue

ensure_in_syspath('../../')


class SaltJobManagerTest(TestCase):
    '''
    Test salt job manager
    '''
    def test_no_pending_jobs(self):
        '''
        Test paths when there are no jobs pending
        '''
        job_mgr = SaltJobManager(10)
        to_del = job_mgr.submit_pending(deque())
        self.assertEqual(to_del, [])
        # make sure nothing is added to the run queue
        self.assertEqual(len(job_mgr.run_queue), 0)

    def test_pending_jobs(self):
        '''
        Test paths when there are pending jobs
        '''
        job_mgr = SaltJobManager(10)
        to_del = job_mgr.submit_pending(deque([1, 2, 3, 4]))
        self.assertEqual(to_del, [1, 2, 3, 4])
        # make sure jobs are added to the run queue
        self.assertEqual(len(job_mgr.run_queue), 4)

    def test_run_queue_full(self):
        '''
        Test it doesn't add when run queue is full
        '''
        job_mgr = SaltJobManager(4)
        # fill up the run queue
        job_mgr.submit_pending(deque([1, 2, 3, 4]))

        # try to submit more jobs
        to_del = job_mgr.submit_pending(deque([1, 2, 3, 4]))
        self.assertEqual(to_del, [])
        self.assertEqual(len(job_mgr.run_queue), 4)

    def test_poll_calls_queue_runner(self):
        '''
        Make sure that the queue runner is called
        to get a list of pending jobs
        '''
        salt.request_queuing.salt_job_manager.__opts__ = {
            'input_queue': {
                'name': 'input_queue',
                'backend': 'sqlite'
            }
        }
        mgr = SaltJobManager(1)
        mock_list = MagicMock(return_value=[])
        mock_del = MagicMock(return_value=[])
        salt.request_queuing.salt_job_manager.__salt__ = {
            'queue.list_items': mock_list,
            'queue.delete': mock_del
        }
        mgr.poll()
        self.assertEqual(mock_list.called, True)
        self.assertEqual(mock_del.called, False)

    def test_calls_queue_delete(self):
        '''
        Make sure that the queue delete path is called
        after jobs are submitted
        '''
        salt.request_queuing.salt_job_manager.__opts__ = {
            'input_queue': {
                'name': 'input_queue',
                'backend': 'sqlite'
            }
        }
        mgr = SaltJobManager(1)
        mock_list = MagicMock(return_value=[1, 2, 3])
        mock_del = MagicMock(return_value=[])
        salt.request_queuing.salt_job_manager.__salt__ = {
            'queue.list_items': mock_list,
            'queue.delete': mock_del
        }
        mgr.poll()
        self.assertEqual(mock_del.called, True)


if __name__ == '__main__':
    from integration import run_tests
    run_tests(SaltJobManagerTest, needs_daemon=False)
