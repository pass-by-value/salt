# -*- coding: utf-8 -*-
'''
Custom runner to interface with PXE server

    Please add the following settings to the salt master config file

    .. code-block:: yaml
        cse_pxe_server_settings:
          ip: '1.2.3.4'
          username: salt
          password: salt
          shared_folder: /path/to/shared/folder
          clonezilla_image_map:
            Windows7: '123456-00'
            CentOS: '479356-49586'

          # mac to blade info mapping
          blades:
            '00-01-01-02-03':
              ip: '1.2.3.4'
              username: blade1user
              password: blade1pass
            '00-01-01-02-04':
              ip: '5.6.7.8'
              username: blade2user
              password: blade2pass

'''
# import python libs
from __future__ import absolute_import
from __future__ import print_function
import logging
import six

# import salt libs
from salt.utils.cloud import wait_for_port, wait_for_winrm
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
    for mac in six.iterkeys(
        __opts__['cse_pxe_server_settings']['blades']):
            blade = __opts__['cse_pxe_server_settings'][
                'blades'][mac]
            if not __salt__['drac.version'](
                blade['ip'],
                blade['username'],
                blade['password'],
            ):
                return mac, blade['ip']


def write_file_to_pxe_server(filepath='', contents=''):
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
        __opts__.get(
            'cse_pxe_server_settings'
        ).get('ip'),
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
    '''
    # TODO: Verify that this config is correct. How do I specify os?
    pxe_settings = __opts__.get('cse_pxe_server_settings')
    ip = pxe_settings['ip']
    username = pxe_settings['username']
    password = pxe_settings['password']
    shared_folder = pxe_settings['shared_folder']
    clonezilla_image_guid = \
        pxe_settings['clonezilla_image_map'].get(
            'os_name',
            'INVALID')

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


def set_pxe_boot(ip_address):
    '''
    Set the boot order to PXE for this blade
    :param hostname: The blade hostname
    '''
    log.info('Setting boot order to PXE')
    _get_runner_client().cmd(
        'drac.pxe',
        args=[ip_address]
    )


def reboot_blade(ip_address):
    '''
    Reboot this blade
    :param hostname: The blade hostname
    '''
    _get_runner_client().cmd(
        'drac.reboot',
        args=[ip_address]
    )
    log.info('Blade was rebooted!')


def poweroff_blade(ip_address):
    '''
    Reboot this blade
    :param hostname: The blade hostname
    '''
    _get_runner_client().cmd(
        'drac.poweroff',
        args=[ip_address]
    )
    log.info('Blade was shut down!')


def wait_for_guest_os(ip_address):
    '''
    Loops until Clonezilla live isn't running and the
    actual guest os is running
    '''
    # TODO: Figure out how to do this
    wait_for_winrm(ip_address)


def wait_for_cloning_to_start(ip_address):
    '''
    Loops until cloning starts
    '''
    # TODO: Figure out how to do this
    wait_for_port(ip_address)


def write_default_file(filepath):
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
        __opts__.get(
            'cse_pxe_server_settings'
        ).get('ip'),
        filepath,
        args=[default_file_contents]
    )
    log.info('Wrote default config file to pxe server')


def provision_os(os_name):
    '''
    This top level method is used to determine
    1. The available blade
    2. Write config file to pxe server
    3. Set boot order for blade to pxe and reboot
    4. Wait for the cloning process to start
    5. Write default file to pxe server
    5. Wait for the Clonezilla os to be replaced by the guest os
    '''
    mac, ip_address = get_available_blade()
    write_file_to_pxe_server(filename=mac,
                             filepath=get_file_path(os_name),
                             file_contents=get_file_contents(os_name))

    set_pxe_boot(ip_address)
    reboot_blade(ip_address)

    wait_for_cloning_to_start(ip_address)
    write_default_file(get_file_path(os_name))

    wait_for_guest_os(ip_address)

    # TODO: What are the next steps that we have to take before testing can start?

    poweroff_blade(ip_address)
