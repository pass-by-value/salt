# -*- coding: utf-8 -*-
'''
Test cases for event helper
'''

# Import Python libs
from __future__ import absolute_import

# Import Salt Testing libs
from random import randint
from salttesting.unit import TestCase
from salttesting.helpers import ensure_in_syspath
from salttesting.mock import MagicMock

# Import Salt libs
from salt.request_queuing.event_helper import (
    get_pending_events,
    get_event_source
)

ensure_in_syspath('../../')


class EventHelperTest(TestCase):
    '''
    Test event helper for request queue
    '''
    def test_returns_empty_list_if_no_events(self):
        '''
        Test when there are no events to return
        '''
        event_source = get_event_source()
        event_source.get_event = MagicMock(return_value=None)
        self.assertEqual(get_pending_events(event_source), [])

    def test_returns_events_if_any(self):
        '''
        Test when there are no events to return
        '''
        event_source = get_event_source()
        ret = {'a': 1}
        # Check with just one event
        event_source.get_event = MagicMock(side_effect=[ret, None])
        self.assertEqual(get_pending_events(event_source), [ret])

        event_source = get_event_source()
        ret = {'a': 1}
        # Check with more than one event
        events = [ret] * randint(2, 20)
        event_source.get_event = MagicMock(side_effect=events + [None])
        self.assertEqual(get_pending_events(event_source), events)

if __name__ == '__main__':
    from integration import run_tests
    run_tests(EventHelperTest, needs_daemon=False)
