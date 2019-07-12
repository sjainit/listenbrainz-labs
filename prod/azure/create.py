import prod.config as config

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient



VM_DETAILS = {
    'publisher': 'Canonical',
    'offer': 'UbuntuServer',
    'sku': '18.04.0-LTS',
    'version': 'latest',
}

GROUP_NAME = 'param-lb-labs-resource-group'
LOCATION = 'centralindia'
VNET_NAME = 'param-lb-labs-virtual-net'
SUBNET_NAME = 'param-lb-labs-subnet'
NIC_NAME = 'param-lb-labs-nic'
IP_CONFIG_NAME = 'param-lb-labs-ip-config'

def get_credentials(client_id, client_secret, tenant_id):
    return ServicePrincipalCredentials(
        client_id=client_id,
        secret=client_secret,
        tenant=tenant_id,
    )


def create_nic(network_client):
    print("Creating virtual network...")
    async_vnet_creation = network_client.virtual_networks.create_or_update(GROUP_NAME, VNET_NAME, {
        'location': LOCATION,
        'address_space': {
            'address_prefixes': ['10.0.0.0/16'],
        },
    })
    async_vnet_creation.wait()
    print("Done!")

    print("Creating Subnet...")
    async_subnet_creation = network_client.subnets.create_or_update(
        GROUP_NAME,
        VNET_NAME,
        SUBNET_NAME,
        {'address_prefix': '10.0.0.0/24'},
    )
    subnet_info = async_subnet_creation.result()
    print("Done!")

    async_nic_creation = network_client.network_interfaces.create_or_update(GROUP_NAME, NIC_NAME, {
        'location': LOCATION,
        'ip_configurations': [{
            'name': IP_CONFIG_NAME,
            'subnet': {
                'id': subnet_info.id,
            },
        }],
    })
    return async_nic_creation.result()


def create_cluster():
    credentials = get_credentials(
        client_id=config.AZURE_CLIENT_ID,
        client_secret=config.AZURE_CLIENT_SECRET,
        tenant_id=config.AZURE_TENANT_ID,
    )

    print("Creating resource group...")
    resource_client = ResourceManagementClient(credentials, config.AZURE_SUBSCRIPTION_ID)
    resource_client.resource_groups.create_or_update(GROUP_NAME, {
        'location': LOCATION,
    })
    print("Resource group created!")

    network_client = NetworkManagementClient(credentials, config.AZURE_SUBSCRIPTION_ID)
    nic = create_nic(network_client)
