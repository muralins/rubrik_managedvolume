# Script which creates the Managed Volume, waits till the MV is created and then displays all the info related to an MV Setup
# Author: Murali.Sriram@rubrik.com
# Usage: python mv_create.py <MV_NAME>

import rubrik_cdm
import click
import json
import urllib3
import base64
import time

urllib3.disable_warnings()

with open('config.json') as config_file:
    config = json.load(config_file)
if not config['rubrik_cdm_node_ip']:
    config['rubrik_cdm_node_ip'] = None
rubrik = rubrik_cdm.Connect(config['rubrik_cdm_node_ip'], config['rubrik_cdm_username'], config['rubrik_cdm_password'],
                            config['rubrik_cdm_token'])

@click.command()
@click.argument('managed_volume_name')
def main(managed_volume_name):
    print("Creating managed volume '{}'".format(managed_volume_name))

    payload = {
        "name": managed_volume_name,
        "applicationTag": config['applicationTag'],
        "numChannels": config['numChannels'],
        "volumeSize": config['volumeSize'],
        "exportConfig": {
            "shareType": "NFS"
        }
    }
    if config['subnet'] is not None and config['subnet'] != "":
        payload['subnet'] = config['subnet']
        payload['exportConfig']['subnet'] = config['subnet']
    create_managed_volume = rubrik.post('internal', '/managed_volume', payload, timeout=60)

    print("Managed Volume created Successfully '{}'".format(managed_volume_name))

    managed_volume_info = get_managed_volume_info(managed_volume_name)

    while (managed_volume_info['state'] != 'Exported'):
            print("Managed Volume is still in state {}...sleeping for 30 seconds".format(managed_volume_info['state']))
            managed_volume_info = get_managed_volume_info(managed_volume_name)
            time.sleep(30)

    if managed_volume_info['isWritable']:
        snapshot_state = "Writable"
    else:
        snapshot_state = "Read Only"
    print("The current state of the {} managed volume is {} and the managed volume is {}".format(managed_volume_info['name'], managed_volume_info['state'], snapshot_state))

    print_managed_volume_setup(managed_volume_info)

    if config['rubrik_cdm_username'] and config['rubrik_cdm_password']:
        print_managed_volume_snapshot(rubrik.username, rubrik.password, rubrik.node_ip, managed_volume_info['id'])
    else:
        print_managed_volume_snapshot_token(rubrik.api_token,rubrik.node_ip,managed_volume_info['id'])

def get_managed_volume_info(managed_volume_name):
    # Get managed volume id
    managed_volume_id = rubrik.object_id(managed_volume_name, 'managed_volume')
    # Get the managed volume details
    managed_volume_info = rubrik.get('internal', '/managed_volume/{}'.format(managed_volume_id))
    return managed_volume_info

def print_managed_volume_setup(managed_volume_info):
    print('-' * 50)
    print(managed_volume_info['id'])
    print('-' * 50)
    if managed_volume_info['state'] == 'Exported':
        print("# Add these lines to /etc/fstab on linux hosts")
        for number, channel in enumerate(managed_volume_info['mainExport']['channels']):
            print("{}:{}  {}/{}-ch{}  nfs {} 0 0".format(
                channel['ipAddress'],
                channel['mountPoint'],
                config['nfs_mount_path'], managed_volume_info['name'], number, config['nfs_mount_options']))
        print('-' * 50)
        print("# Make the mount points (run as root user) and give permissions to Oracle user chown -R oracle:oinstall /mnt/rubrik/*")
        for number, channel in enumerate(managed_volume_info['mainExport']['channels']):
            print("mkdir -p {}/{}-ch{}".format(config['nfs_mount_path'], managed_volume_info['name'], number))

        print('-' * 50)
        print("# Mount the NFS exports.")
        for number, channel in enumerate(managed_volume_info['mainExport']['channels']):
            print("mount {}/{}-ch{}".format(config['nfs_mount_path'], managed_volume_info['name'], number))
        print('-' * 50)
        print("# RMAN channels to use in backup scripts:")
        for number, channel in enumerate(managed_volume_info['mainExport']['channels']):
            print(
                "allocate channel ch{} device type disk format '{}/{}-ch{}/%U';".format(number, config['nfs_mount_path'],
                                                                                        managed_volume_info['name'], number))
        print('-' * 50)
    else:
        print("The managed volume has not been exported yet")

def print_managed_volume_snapshot_token(api_token, rubrik_ip, managed_volume_id):
    print("# The begin snapshot ReST API command is:")
    print(
        "curl -k -X POST -H 'Authorization: Bearer {}' 'https://{}/api/internal/managed_volume/{}/begin_snapshot'".format(
              api_token, rubrik_ip, managed_volume_id))
    print("# The end snapshot ReST API command is:")
    print(
        "curl -k -X POST -H 'Authorization: Bearer {}' 'https://{}/api/internal/managed_volume/{}/end_snapshot'".format(
             api_token, rubrik_ip, managed_volume_id))

def print_managed_volume_snapshot(username, password, rubrik_ip, managed_volume_id):
    user_pass = username + ':' + password
    b_user_pass = user_pass.encode()
    enc_user_pass = base64.b64encode(b_user_pass).decode()
    print("# Rubrik user in snapshot command: {}".format(username))
    print("# The begin snapshot ReST API command is:")
    print(
        "curl -k -X POST -H 'Authorization: Basic {}' 'https://{}/api/internal/managed_volume/{}/begin_snapshot'".format(
             enc_user_pass, rubrik_ip, managed_volume_id))
    print("# The end snapshot ReST API command is:")
    print(
        "curl -k -X POST -H 'Authorization: Basic {}' 'https://{}/api/internal/managed_volume/{}/end_snapshot'".format(
            enc_user_pass, rubrik_ip, managed_volume_id))

if __name__ == "__main__":
    main()