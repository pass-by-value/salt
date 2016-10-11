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
import salt.config

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def get_available_blade():
    '''
    Method to determine which blade is available
    '''
    # TODO: Implement this and test with actual setup
    # how to determine which blade is available?
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
    # TODO: Implement this
    # how to determine the file path?
    log.info('Called function to get file path')
    return options.get('filename', '')


def get_file_contents(os_name):
    '''
    Return the config file contents

    Please add the following settings to the salt master config file

    .. code-block:: yaml
        cse_pxe_server_settings:
          ip: '1.2.3.4'
          username: salt
          password: salt
          shared_folder: /path/to/shared/folder
          clonezilla_image_guid: '123456-00'

    '''
    # TODO: Verify that this config is correct. How do I specify os?
    pxe_settings = __opts__.get('cse_pxe_server_settings')
    ip = pxe_settings['ip']
    username = pxe_settings['username']
    password = pxe_settings['password']
    shared_folder = pxe_settings['shared_folder']
    clonezilla_image_guid = pxe_settings['clonezilla_image_guid']

    return 'DEFAULT clonezilla' \
           'PROMPT 0' \
           'NOESCAPE 0' \
           'ALLOWOPTIONS 0' \
           'TIMEOUT 1' \
           '\n' \
           'LABEL local' \
           'LOCALBOOT 0' \
           '\n' \
           'LABEL memdisk' \
           'kernel image/memdisk' \
           '\n' \
           'LABEL clonezilla' \
           'KERNEL clonezilla/vmlinuz' \
           '\n' \
           'APPEND initrd=clonezilla/initrd.img ' \
           'boot=live config noswap nolocales union=aufs edd=on ' \
           'nomodeset keyboard-layouts=NONE ' \
           'nosplash noprompt ' \
           'fetch=ftp://{0}/filesystem.squashfs ' \
           'ocs_prerun="mount -t cifs ' \
           '-o user={1},password={2} ' \
           '//{0}/{3} ' \
           '/home/partimag" ' \
           'ocs_live_run="osc-sr ' \
           '-g auto -e1 auto -e2 ' \
           '--batch -j2 -p reboot ' \
           'restoredisk {4} sda"'.format(
                ip, username, password, shared_folder, clonezilla_image_guid
            )


def _get_runner_client():
    '''
    Instantiate and return the runner client
    '''
    return salt.client.RunnerClient(
        salt.config.client_config('/etc/salt/master')
    )


def set_pxe_boot(hostname):
    '''
    Set the boot order to PXE for this blade
    :param hostname: The blade hostname
    '''
    _get_runner_client().cmd(
        'drac.pxe',
        args=[hostname]
    )


def reboot_blade(hostname):
    '''
    Reboot this blade
    :param hostname: The blade hostname
    '''
    _get_runner_client().cmd(
        'drac.reboot',
        args=[hostname]
    )


def wait_for_guest_os():
    '''
    Loops until Clonezilla live isn't running and the
    actual guest os is running
    '''
    # TODO: Figure out how to do this
    pass


def wait_for_cloning_to_start():
    '''
    Loops until cloning starts
    '''
    # TODO: Figure out how to do this
    pass


def write_default_file(pxe_server, filepath):
    '''
    Write the default file to pxe server
    :param pxe_server: Host name of the PXE server
    '''
    log.info('Writing default config file to pxe server')
    local = salt.client.LocalClient()
    default_file_contents = 'DEFAULT local' \
                            'LABEL local' \
                            'LOCALBOOT 0'
    local.cmd(
        pxe_server,
        filepath,
        args=[default_file_contents]
    )
    log.info('Wrote default config file to pxe server')


def provision_os(os_name, hostname):
    '''
    This top level method is used to determine
    1. The available blade
    2. Write config file to pxe server
    3. Set boot order for blade to pxe and reboot
    4. Wait for the cloning process to start
    5. Write default file to pxe server
    5. Wait for the Clonezilla os to be replaced by the guest os
    '''
    blade = get_available_blade()
    write_file_to_pxe_server(hostname,
                             filename=blade['mac'],
                             filepath=get_file_path(os_name),
                             file_contents=get_file_contents(os_name))

    set_pxe_boot(hostname)
    reboot_blade(hostname)

    wait_for_cloning_to_start()
    write_default_file(hostname,
                       get_file_path(os_name))

    wait_for_guest_os()
    # TODO: What are the next steps that we have to take before testing can start?
