# -*- coding: utf-8 -*-
'''
    tests.unit.run_queuing.salt_request_manager_test
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
'''

# Import Python libs
from __future__ import absolute_import

# Import Salt Testing libs
from salttesting.unit import TestCase
from salttesting.helpers import ensure_in_syspath
from salttesting.mock import MagicMock
import six

# Import salt libs
from salt.config import master_config
from salt.request_queuing.salt_request_manager import SaltRequestManager
from .event_data import get_events

ensure_in_syspath('../../')


class SaltRequestManagerTestCase(TestCase):
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
        reader = MagicMock()
        reader.save_request = MagicMock()
        req_mgr = SaltRequestManager(opts, reader)
        # Create a new request object internally
        # and return the request id
        req_id = req_mgr.initialize_request(
            'foo', {
                'fun':      'jobs.list_jobs',
                'client':   'runner',
            }
        )
        self.assertEqual(len(req_id), 20)
        # Assert that save to db is called
        reader.save_request.assert_called()

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
        req_mgr = SaltRequestManager(opts, MagicMock())

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
        queue_reader.save_request = lambda x: x

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
        queue_reader.save_request = lambda x: x  # Don't care what this does

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
        req_mgr = SaltRequestManager(
            opts,
            queue_reader=MagicMock()
        )

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
        queue_reader.save_request = MagicMock()

        manager = SaltRequestManager(opts, queue_reader=queue_reader)
        manager.poll()
        # make sure that only one job was submitted
        queue_reader.delete_jobs.assert_called_with({'foo': [req_id]})

    def test_update(self):
        '''
        Test the update method
        1. Removes request from run queue - that way new ones can be added
        2. Removes request from known requests - so we don't accumulate memory
           over time
        3. Removes entries from jid_req_map - so we don't accumulate memory
           over time
        '''
        opts = master_config('/etc/salt/master')
        opts.update({
            'input_queues': [{
                'name': 'foo',
                'capacity': 16,
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
        # Now we have requests in the run queue
        request = manager.get_request('foo', req_id)[0]
        manager.event_processor.get_pending_events =\
            MagicMock(return_value=get_events(request['jid']))
        manager.update()

        # Make sure that the job is removed from run queue
        self.assertEqual(request['jid'] in manager.queues['foo'],
                         False)
        # Remove this job from requests
        self.assertEqual(request['request_id'] in manager.requests, False)
        # Remove this job from map
        self.assertEqual(request['jid'] in manager.jid_req_map, False)

if __name__ == '__main__':
    from integration import run_tests
    run_tests(SaltRequestManagerTestCase, needs_daemon=False)
