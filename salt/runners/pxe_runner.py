# -*- coding: utf-8 -*-
'''
Custom runner to interface with PXE server
'''
# import python libs
from __future__ import absolute_import
from __future__ import print_function
import logging

# import salt libs
import salt.client

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def get_available_blade():
    '''
    Method to determine which blade is available
    '''
    # TBD: Implement this and test with actual setup
    log.info('Trying to get an available blade')
    return {
        'mac': '00-01-02-03-04-05-06'
    }


def write_file_to_pxe_server(pxe_server, filepath='', contents=''):
    '''
    Calls salt's file.write module to write a file on the PXE server
    :param pxe_server: Target for the PXE server
    :type str
    :param filepath: Path for the file on the PXE server
    :type str
    :param contents: Contents of the file to write
    :type str
    '''
    log.info('Writing config file to pxe server')
    local = salt.client.LocalClient()
    local.cmd(
        pxe_server,
        filepath,
        args=[contents]
    )
    log.info('Wrote config file to pxe server')


def get_file_path(options):
    '''
    Determine path of this config file on the server
    '''
    # TBD: Implement this
    log.info('Called function to get file path')
    return options.get('filename', '')


def get_file_contents(os_name):
    '''
    Return the config file contents
    '''
    # TBD: Figure out how to specify the OS
    return 'label Clonezilla-live' \
           'MENU LABEL Clonezilla Live (Ramdisk)' \
           'KERNEL vmlinuz' \
           'APPEND initrd=initrd.img ' \
           'boot=live username=user ' \
           'union=overlay config components ' \
           'quiet noswap edd=on nomodeset ' \
           'nodmraid locales= keyboard-layouts= ocs_live_run=' \
           '"ocs-live-general" ocs_live_extra_param="' \
           '" ocs_live_batch=no net.ifnames=0 ' \
           'nosplash noprompt ' \
           'fetch=tftp://$serverIP/filesystem.squashfs'


def provision_os(os_name):
    '''
    This top level method is used to determine
    which blade is available and then
    update the pxe server with the config
    necessary for the right os
    '''
    blade = get_available_blade()
    write_file_to_pxe_server(filename=blade['mac'],
                             filepath=get_file_path(os_name),
                             file_contents=get_file_contents(os_name))
