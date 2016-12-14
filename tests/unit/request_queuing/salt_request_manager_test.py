# -*- coding: utf-8 -*-
'''
    tests.unit.run_queuing.salt_job_manager_test
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
'''

# Import Python libs
from __future__ import absolute_import
import six
# Import Salt Testing libs
from salttesting.unit import TestCase
from salttesting.helpers import ensure_in_syspath
from salttesting.mock import MagicMock

# Import salt libs
from salt.config import master_config
from salt.request_queuing.salt_request_manager import SaltRequestManager
from .event_data import get_events

ensure_in_syspath('../../')


class SaltRequestManagerTest(TestCase):
    '''
    Test salt request manager
    '''
    def test_it_works_when_no_queues_defined(self):
        '''
        Test edge cases
        '''
        opts = master_config('/etc/salt/master')
        req_mgr = SaltRequestManager(opts)
        self.assertNotEqual(req_mgr, None)
        self.assertEqual(req_mgr.queues, {})
        self.assertEqual(req_mgr.requests, {})
        self.assertEqual(req_mgr.input_processors, {})

    def test_salt_request_manager_initialization(self):
        '''
        Test paths when there are no jobs pending
        '''
        opts = master_config('/etc/salt/master')
        opts.update({
            'input_queues': [{
                'name': 'foo',
                'capacity': 16,
            }]
        })
        req_mgr = SaltRequestManager(opts)
        self.assertNotEqual(req_mgr, None)
        self.assertEqual(len(req_mgr.queues), 1)
        self.assertNotEqual(req_mgr.input_processors['foo'], None)

    def test_initialize_request(self):
        '''
        Tests new request initilization
        '''
        opts = master_config('/etc/salt/master')
        opts.update({
            'input_queues': [{
                'name': 'foo',
                'capacity': 16,
            }]
        })
        req_mgr = SaltRequestManager(opts)
        # Create a new request object internally
        # and return the request id
        req_id = req_mgr.initialize_request(
            'foo', {
                'fun':      'jobs.list_jobs',
                'client':   'runner',
            }
        )
        self.assertEqual(len(req_id), 20)

        # check that the correct data structure is returned
        self.assertEqual(
            req_mgr.get_request('foo',  req_id),
            [{
                'state': 'new',
                'jid': None,
                'request_id': req_id,
                'low': {
                    'fun': 'jobs.list_jobs',
                    'client': 'runner'
                },
                'input_queue': 'foo'
            }]
        )

    @staticmethod
    def _queue_reader():
        '''
        Queue reader to return data structure
        with updated queues
        :return: Dict keyed by input_queue name
        :rtype Dict
        '''
        opts = master_config('/etc/salt/master')
        opts.update({
            'input_queues': [{
                'name': 'foo',
                'capacity': 16,
            }, {
                'name': 'bar',
                'capacity': 7,
            }]
        })
        req_mgr = SaltRequestManager(opts)

        id_foo1 = req_mgr.initialize_request('foo', {
            'fun': 'foo.bar',
            'client': 'runner',
        })
        id_foo2 = req_mgr.initialize_request('foo', {
            'fun': 'jobs.list_jobs',
            'client': 'runner',
        })
        id_bar = req_mgr.initialize_request('bar', {
            'fun': 'jobs.list_jobs',
            'client': 'runner',
        })

        foo_req = req_mgr.get_request('foo', id_foo1)
        foo_req.extend(req_mgr.get_request('foo', id_foo2))
        bar_req = req_mgr.get_request('bar', id_bar)

        # Expected format allowing batch operation
        return {'foo': foo_req, 'bar': bar_req}

    def test_poll(self):
        '''
        Test that it gets messages
        from input queues and processes them
        '''
        opts = master_config('/etc/salt/master')
        opts.update({
            'input_queues': [{
                'name': 'foo',
                'capacity': 16,
            }, {
                'name': 'bar',
                'capacity': 16,
            }]
        })
        queue_reader = MagicMock()
        # this supplies requests
        queue_reader.read_all_queues = self._queue_reader
        queue_reader.delete_jobs = lambda x: x

        manager = SaltRequestManager(opts, queue_reader)
        manager.poll()
        self.assertEqual(len(dict(manager.queues)['foo']), 2)
        self.assertEqual(len(dict(manager.queues)['bar']), 1)
        self.assertEqual(len(list(six.iterkeys(manager.jid_req_map))), 3)

    def test_does_not_submit_more_jobs_than_capacity(self):
        '''
        Test that it gets messages
        from input queues and processes them
        '''
        opts = master_config('/etc/salt/master')
        opts.update({
            'input_queues': [{
                'name': 'foo',
                'capacity': 1,
            }]
        })
        queue_reader = MagicMock()
        queue_reader.read_all_queues = self._queue_reader
        queue_reader.delete_jobs = lambda x: x  # Don't care what this does

        manager = SaltRequestManager(opts, queue_reader)
        manager.poll()
        # make sure that only one job was submitted
        self.assertEqual(len(dict(manager.queues)['foo']), 1)

    @staticmethod
    def _queue_with_jobs():
        '''
        Queue reader to return data structure
        with updated queues
        :return: Dict keyed by input_queue name
        :rtype Dict
        '''
        opts = master_config('/etc/salt/master')
        opts.update({
            'input_queues': [{
                'name': 'foo',
                'capacity': 16,
            }]
        })
        req_mgr = SaltRequestManager(opts)

        id_foo1 = req_mgr.initialize_request('foo', {
            'fun': 'foo.bar',
            'client': 'runner',
        })
        id_foo2 = req_mgr.initialize_request('foo', {
            'fun': 'jobs.list_jobs',
            'client': 'runner',
        })

        foo_req = req_mgr.get_request('foo', id_foo1)
        foo_req.extend(req_mgr.get_request('foo', id_foo2))
        return id_foo1, {'foo': foo_req}

    def test_calls_delete_on_submitted_jobs(self):
        '''
        Input queue cleanup testing
        :return:
        '''
        opts = master_config('/etc/salt/master')
        opts.update({
            'input_queues': [{
                'name': 'foo',
                'capacity': 1,
            }]
        })
        queue_reader = MagicMock()
        queue_reader.read_all_queues = self._queue_with_jobs
        queue_reader.delete_jobs = MagicMock()

        req_id, data = self._queue_with_jobs()
        queue_reader = MagicMock()
        # this supplies requests
        queue_reader.read_all_queues = MagicMock(return_value=data)
        queue_reader.delete_jobs = MagicMock()

        manager = SaltRequestManager(opts, queue_reader)
        manager.poll()
        # make sure that only one job was submitted
        queue_reader.delete_jobs.assert_called_with({'foo': [req_id]})

    def test_update(self):
        '''
        Test the update method marks jobs as complete
        '''
        opts = master_config('/etc/salt/master')
        opts.update({
            'input_queues': [{
                'name': 'foo',
                'capacity': 16,
            }, {
                'name': 'bar',
                'capacity': 16,
            }]
        })
        queue_reader = MagicMock()
        # this supplies requests
        queue_reader.read_all_queues = self._queue_reader
        queue_reader.delete_jobs = lambda x: x

        manager = SaltRequestManager(opts, queue_reader)
        manager.poll()
    # def test_pending_jobs(self):
    #     '''
    #     Test paths when there are pending jobs
    #     '''
    #     job_mgr = SaltJobManager(10)
    #     job_mgr.submit_one = lambda x: x
    #     to_del = job_mgr.submit_pending(deque([1, 2, 3, 4]))
    #     self.assertEqual(to_del, [1, 2, 3, 4])
    #     # make sure jobs are added to the run queue
    #     self.assertEqual(len(job_mgr.run_queue), 4)
    #
    # def test_run_queue_full(self):
    #     '''
    #     Test it doesn't add when run queue is full
    #     '''
    #     job_mgr = SaltJobManager(4)
    #     job_mgr.submit_one = lambda x: x
    #     # fill up the run queue
    #     job_mgr.submit_pending(deque([1, 2, 3, 4]))
    #
    #     # try to submit more jobs
    #     to_del = job_mgr.submit_pending(deque([1, 2, 3, 4]))
    #     self.assertEqual(to_del, [])
    #     self.assertEqual(len(job_mgr.run_queue), 4)
    #
    # def test_poll_calls_queue_runner(self):
    #     '''
    #     Make sure that the queue runner is called
    #     to get a list of pending jobs
    #     '''
    #     opts = {
    #         'input_queue': {
    #             'name': 'input_queue',
    #             'backend': 'sqlite',
    #         }
    #     }
    #     mock_list = MagicMock(return_value=[])
    #     mock_del = MagicMock(return_value=[])
    #     runners = {
    #         'queue.list_items': mock_list,
    #         'queue.delete': mock_del,
    #     }
    #     mgr = SaltJobManager(1, opts=opts, runners=runners)
    #     mgr.submit_one = lambda x: x
    #     mgr.poll()
    #     self.assertEqual(mock_list.called, True)
    #     self.assertEqual(mock_del.called, False)
    #
    # def test_calls_queue_delete(self):
    #     '''
    #     Make sure that the queue delete path is called
    #     after jobs are submitted
    #     '''
    #     opts = {
    #         'input_queue': {
    #             'name': 'input_queue',
    #             'backend': 'sqlite',
    #         }
    #     }
    #     mock_list = MagicMock(return_value=[1, 2, 3])
    #     mock_del = MagicMock(return_value=[])
    #     runners = {
    #         'queue.list_items': mock_list,
    #         'queue.delete': mock_del,
    #     }
    #     mgr = SaltJobManager(capacity=1, opts=opts, runners=runners)
    #     mgr.submit_one = lambda x: x
    #     mgr.poll()
    #     self.assertEqual(mock_del.called, True)
    #
    # def test_it_calls_async_on_runner_client(self):
    #     '''
    #     Instantiate it can be instantiated
    #     with a runner client
    #     '''
    #     # runner_client = RunnerClient(master_config('/etc/salt/master'))
    #     runner_client = MagicMock()
    #     runner_client.async = MagicMock(return_value={
    #         'jid': '20161103110134203443',
    #         'tag': 'salt/run/20161103110134203443',
    #     })
    #     mgr = SaltJobManager(runner_client=runner_client)
    #     requests = deque([{
    #         'low': '123'
    #     }])
    #     mgr.submit_pending(requests)
    #     self.assertEqual(runner_client.async.called, True)
    #
    # def test_it_pops_jobs_off_the_run_queue(self):
    #     '''
    #     Make sure that finished jobs are removed from
    #     the run queue and jobs that are still running
    #     stay in the run queue
    #     '''
    #     runners = {
    #         'jobs.list_jobs': MagicMock(return_value={'1': {}, '2': {}})
    #     }
    #     opts = {
    #         'metadata': ''
    #     }
    #     mgr = SaltJobManager(
    #         runner_client=MagicMock(),
    #         opts=opts,
    #         runners=runners
    #     )
    #     mgr.run_queue.add('1')
    #     mgr.run_queue.add('2')
    #     mgr.run_queue.add('3')
    #
    #     mgr.update()
    #     self.assertEqual(len(mgr.run_queue), 1)
    #     self.assertEqual('1' in mgr.run_queue, False)
    #     self.assertEqual('2' in mgr.run_queue, False)
    #     self.assertEqual('3' in mgr.run_queue, True)

if __name__ == '__main__':
    from integration import run_tests
    run_tests(SaltRequestManagerTest, needs_daemon=False)
