# -*- coding: utf-8 -*-
'''
Event data fixture.
'''


def get_events(jid):
    '''
    Returns events for a given jid
    '''
    return [{
        'tag': jid,
        'data': {
            '_stamp': '2016-12-08T16:47:05.305210',
            'minions': ['abstraction.local']
        }
    }, {
        'tag': 'salt/job/{0}/new'.format(jid),
        'data': {
            'tgt_type': 'glob',
            'jid': '{0}'.format(jid),
            'tgt': '*.local',
            '_stamp': '2016-12-08T16:47:05.305601',
            'user': 'sudo_adi',
            'arg': [],
            'fun': 'test.ping',
            'minions': ['abstraction.local']
        }
    }, {
        'tag': 'salt/job/{0}/ret/abstraction.local'.format(jid),
        'data': {
            'fun_args': [],
            'jid': jid,
            'return': True,
            'retcode': 0,
            'success': True,
            'cmd': '_return',
            '_stamp': '2016-12-08T16:47:05.374480',
            'fun': 'test.ping',
            'id': 'abstraction.local',
            'metadata': {
                'foo': 'bar'
            }
        }
    }]
