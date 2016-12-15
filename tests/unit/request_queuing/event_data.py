# -*- coding: utf-8 -*-
'''
Event data fixture.
'''

from __future__ import absolute_import


def get_events(jid):
    '''
    Returns events for a given jid
    '''
    return [{
        'tag': jid,
        'data': {
            '_stamp': '2016-12-08T16:47:05.305210',
            'minions': ['saltvm']
        }
    }, {
        'tag': 'salt/job/{0}/new'.format(jid),
        'data': {
            'tgt_type': 'glob',
            'jid': '{0}'.format(jid),
            'tgt': '*.local',
            '_stamp': '2016-12-08T16:47:05.305601',
            'user': 'salt',
            'arg': [],
            'fun': 'test.ping',
            'minions': ['saltvm']
        }
    }, {
        'tag': 'salt/job/{0}/ret/saltvm'.format(jid),
        'data': {
            'fun_args': [],
            'jid': jid,
            'return': True,
            'retcode': 0,
            'success': True,
            'cmd': '_return',
            '_stamp': '2016-12-08T16:47:05.374480',
            'fun': 'test.ping',
            'id': 'saltvm',
            'metadata': {
                'foo': 'bar'
            }
        }
    }]
