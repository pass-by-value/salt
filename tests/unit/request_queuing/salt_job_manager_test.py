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
        job_mgr.submit_one = lambda x: x
        self.assertEqual(to_del, [])
        # make sure nothing is added to the run queue
        self.assertEqual(len(job_mgr.run_queue), 0)

    def test_pending_jobs(self):
        '''
        Test paths when there are pending jobs
        '''
        job_mgr = SaltJobManager(10)
        job_mgr.submit_one = lambda x: x
        to_del = job_mgr.submit_pending(deque([1, 2, 3, 4]))
        self.assertEqual(to_del, [1, 2, 3, 4])
        # make sure jobs are added to the run queue
        self.assertEqual(len(job_mgr.run_queue), 4)

    def test_run_queue_full(self):
        '''
        Test it doesn't add when run queue is full
        '''
        job_mgr = SaltJobManager(4)
        job_mgr.submit_one = lambda x: x
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
                'backend': 'sqlite',
            }
        }
        mgr = SaltJobManager(1)
        mgr.submit_one = lambda x: x
        mock_list = MagicMock(return_value=[])
        mock_del = MagicMock(return_value=[])
        salt.request_queuing.salt_job_manager.__salt__ = {
            'queue.list_items': mock_list,
            'queue.delete': mock_del,
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
                'backend': 'sqlite',
            }
        }
        mgr = SaltJobManager(capacity=1)
        mgr.submit_one = lambda x: x
        mock_list = MagicMock(return_value=[1, 2, 3])
        mock_del = MagicMock(return_value=[])
        salt.request_queuing.salt_job_manager.__salt__ = {
            'queue.list_items': mock_list,
            'queue.delete': mock_del,
        }
        mgr.poll()
        self.assertEqual(mock_del.called, True)

    def test_it_calls_async_on_runner_client(self):
        '''
        Instantiate it can be instantiated
        with a runner client
        '''
        # runner_client = RunnerClient(master_config('/etc/salt/master'))
        runner_client = MagicMock()
        runner_client.async = MagicMock(return_value={
            'jid': '20161103110134203443',
            'tag': 'salt/run/20161103110134203443',
        })
        mgr = SaltJobManager(runner_client=runner_client)
        requests = deque([{
            'low': '123'
        }])
        mgr.submit_pending(requests)
        self.assertEqual(runner_client.async.called, True)

    def test_it_pops_jobs_off_the_run_queue(self):
        '''
        Make sure that finished jobs are removed from
        the run queue
        '''
        mgr = SaltJobManager(
            runner_client=MagicMock()
        )
        mgr.run_queue.add('1')
        mgr.run_queue.add('2')
        mgr.run_queue.add('3')

        salt.request_queuing.salt_job_manager.__salt__ = {
            'jobs.list_jobs': MagicMock(return_value={'1': {}, '2': {}})
        }
        salt.request_queuing.salt_job_manager.__opts__ = {
            'metadata': ''
        }
        mgr.update()
        self.assertEqual(len(mgr.run_queue), 1)
        self.assertEqual('1' in mgr.run_queue, False)
        self.assertEqual('2' in mgr.run_queue, False)
        self.assertEqual('3' in mgr.run_queue, True)

if __name__ == '__main__':
    from integration import run_tests
    run_tests(SaltJobManagerTest, needs_daemon=False)
