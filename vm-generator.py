from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient

print(
    "Provisioning a virtual machine...some operations might take a \
minute or two."
)

# Acquire a credential object.
credential = DefaultAzureCredential()
subscription_id = 'aee8556f-d2fd-4efd-a6bd-f341a90fa76e'

RESOURCE_GROUP_NAME = "Data_Engineer"
LOCATION = "westeurope"

# Network and IP address names
VNET_NAME = "stavros-sotiropoulos-vnet"
SUBNET_NAME = "stavros-sotiropoulos-subnet"
NSG_NAME = 'stavros-sotiropoulos-nsg'
IP_NAME = "stavros-sotiropoulos-ip"
IP_CONFIG_NAME = "stavros-sotiropoulos-ip-config"
NIC_NAME = "stavros-sotiropoulos-nic"

VM_NAME = "VM-Stavros"
USERNAME = "stasotiro"
PASSWORD = "StA50tir0@DTU"

def createVirtualNetwork(network_client):
    # Provision the virtual network and wait for completion
    poller = network_client.virtual_networks.begin_create_or_update(
        RESOURCE_GROUP_NAME,
        VNET_NAME,
        {
            "location": LOCATION,
            "address_space": {"address_prefixes": ["10.0.0.0/16"]},
        },
    )

    vnet_result = poller.result()

    print(
        f"Provisioned virtual network {vnet_result.name} with address \
    prefixes {vnet_result.address_space.address_prefixes}"
    )

    # Provision a Network security group
    nsg = network_client.network_security_groups.begin_create_or_update(
        RESOURCE_GROUP_NAME,
        NSG_NAME,
        {
            "location": LOCATION
        }
    ).result()

    # Step 3: Provision the subnet and wait for completion
    poller = network_client.subnets.begin_create_or_update(
        RESOURCE_GROUP_NAME,
        VNET_NAME,
        SUBNET_NAME,
        {
            "address_prefix": "10.0.0.0/24",
            "network_security_group": {
                "id": nsg.id
            }
        },
    )
    subnet_result = poller.result()

    print(
        f"Provisioned virtual subnet {subnet_result.name} with address \
    prefix {subnet_result.address_prefix}"
    )

    # Step 4: Provision an IP address and wait for completion
    poller = network_client.public_ip_addresses.begin_create_or_update(
        RESOURCE_GROUP_NAME,
        IP_NAME,
        {
            "location": LOCATION,
            "sku": {"name": "Standard"},
            "public_ip_allocation_method": "Static",
            "public_ip_address_version": "IPV4",
        },
    )

    ip_address_result = poller.result()

    print(
        f"Provisioned public IP address {ip_address_result.name} \
    with address {ip_address_result.ip_address}"
    )

    # Step 5: Provision the network interface client
    poller = network_client.network_interfaces.begin_create_or_update(
        RESOURCE_GROUP_NAME,
        NIC_NAME,
        {
            "location": LOCATION,
            "ip_configurations": [
                {
                    "name": IP_CONFIG_NAME,
                    "subnet": {"id": subnet_result.id},
                    "public_ip_address": {"id": ip_address_result.id},
                }
            ],
        },
    )

    nic_result = poller.result()

    print(f"Provisioned network interface client {nic_result.name}")
    return nic_result

def createVirtualMachine(nic_result):
    # Step 6: Provision the virtual machine

    # Obtain the management object for virtual machines
    compute_client = ComputeManagementClient(credential, subscription_id)


    print(
        f"Provisioning virtual machine {VM_NAME}; this operation might \
    take a few minutes."
    )

    poller = compute_client.virtual_machines.begin_create_or_update(
        RESOURCE_GROUP_NAME,
        VM_NAME,
        {
            "location": LOCATION,
            "storage_profile": {
                "image_reference": {
                    "publisher": "Canonical",
                    "offer": "UbuntuServer",
                    "sku": "16.04.0-LTS",
                    "version": "latest",
                }
            },
            "hardware_profile": {"vm_size": "Standard_A1_v2"},
            "os_profile": {
                "computer_name": VM_NAME,
                "admin_username": USERNAME,
                "admin_password": PASSWORD,
            },
            "network_profile": {
                "network_interfaces": [
                    {
                        "id": nic_result.id,
                    }
                ]
            },
        },
    )

    vm_result = poller.result()

    print(f"Provisioned virtual machine {vm_result.name}")
    return vm_result


if __name__ == "__main__":

    try:
        
        print("Starting process")
        # Step 1: Provision a resource group

        # Obtain the management object for resources.
        resource_client = ResourceManagementClient(credential, subscription_id)
        # Provision the resource group.
        rg_result = resource_client.resource_groups.create_or_update(
            RESOURCE_GROUP_NAME, {"location": LOCATION}
        )

        print(
            f"Provisioned resource group {rg_result.name} in the \
        {rg_result.location} region"
        )


        # Step 2: provision a virtual network
        print("Creating VN")

        # Obtain the management object for networks
        network_client = NetworkManagementClient(credential, subscription_id)

        nic_result = createVirtualNetwork(network_client)
        print("Creating VM")

        vm_result = createVirtualMachine(nic_result)
        print("Done")
    except Exception as e: 
        print("Error ", e)