# -*- coding: utf-8 -*-
'''
This is a simple proxy-minion designed to connect to and communicate with
the bottle-based web service contained in
https://github.com/salt-contrib/proxyminion_rest_example
'''
from __future__ import absolute_import

# Import python libs
import json
import logging

# Import Salt's libs
from salt.utils.vt_helper import SSHConnection

# This must be present or the Salt loader won't load this module
__proxyenabled__ = ['ssh_sample']

# Want logging!
log = logging.getLogger(__file__)


# This does nothing, it's here just as an example and to provide a log
# entry when the module is loaded.
def __virtual__():
    '''
    Only return if all the modules are available
    '''
    log.info('ssh_sample proxy __virtual__() called...')
    return True

def init(opts):
    pass

def shutdown(opts):
    pass

def grains():
    return {}

def ping():
    return True

def package_list():
    '''
    List "packages" by executing a command via ssh
    '''
    log.info('Called list '*20)
    server = SSHConnection(host='fractionofc', username='proxycmdshell', password='foo')
    out, err = server.sendline('pkg_list')
    # "scrape" and return the right fields as a dict
    return json.loads(out[9:-7])
