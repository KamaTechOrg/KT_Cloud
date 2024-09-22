### Execution Flow for DB Team Tasks Demonstration

# 1. **Step 1: Create the DB Cluster (Sara Lea)**
#    - **Physical Object**: Create a folder representing the **DB Cluster**. 
#    - **Attributes**: Cluster identifier, engine type, cluster nodes, etc.

# 2. **Step 2: Create DB Instances (Yael K and Sarit)**
#    - **Physical Object**: Create two separate directories representing **DB Instances** inside the DB Cluster directory.
#    - **Attributes**: Instance ID, instance type, status, engine, etc.

# 3. **Step 3: Create a DB Proxy (Efrat Ben Abu)**
#    - **Physical Object**: Create a **file** representing the proxy network settings.
#    - **Attributes**: Proxy identifier, proxy status, VPC subnet.

# 4. **Step 4: Create a DB Proxy Endpoint (Sara N)**
#    - **Physical Object**: Create a **file** representing the proxy endpoint configuration inside the proxy directory.
#    - **Attributes**: Endpoint name, address, type, status, etc.

# 5. **Step 5: Create DB Cluster Parameter Group (Tamar Ko)**
#    - **Physical Object**: Create a **file** representing the parameter group settings.
#    - **Attributes**: Parameter group family, description, configuration parameters.

# 6. **Step 6: Create DB Subnet Group (Temima)**
#    - **Physical Object**: Create a **file** representing the DB subnet group configuration.
#    - **Attributes**: Subnet IDs, VPC ID, description.

# 7. **Step 7: Create a DBSecurityGroup (Gili)**
#    - **Physical Object**: Create a **file** representing the security group and rules.
#    - **Attributes**: Group name, description, inbound rules (IP ranges, protocols).

# 8. **Step 8: Create an Event Subscription (Eti)**
#    - **Physical Object**: Create a **file or directory** representing the event subscription details.
#    - **Attributes**: Subscription name, status, list of subscribed events.

# 9. **Step 9: Create Option Group (Shani S)**
#    - **Physical Object**: Create a **file** representing configuration options.
#    - **Attributes**: Group name, description, options for database features.

# 10. **Step 10: Create Snapshot (Yehudit)**
#     - **Physical Object**: Create a **file** representing the snapshot's storage location and metadata.
#     - **Attributes**: Snapshot ID, creation time, source DB instance.

# 11. **Step 11: Create Replica (Lea B)**
#     - **Physical Object**: Create a **file** representing the replica’s settings and metadata.
#     - **Attributes**: Source DB instance, read replica status, replication settings.

### Execution Order Summary:

# 1. **Create DB Cluster** → 
# 2. **Create DB Instances** inside the DB Cluster → 
# 3. **Create DB Proxy** → 
# 4. **Create DB Proxy Endpoint** → 
# 5. **Create DB Cluster Parameter Group** → 
# 6. **Create DB Subnet Group** → 
# 7. **Create DBSecurityGroup** → 
# 8. **Create Event Subscription** → 
# 9. **Create Option Group** → 
# 10. **Create Snapshot** → 
# 11. **Create Replica**

from termcolor import colored


def print_message(message: str, mode: str = None):
    """Print the message in a specific color based on the mode."""
    
    if mode == 'system_info':
        # Print message in yellow
        print(colored(message, 'yellow'))
    elif mode == 'action':
        # Print message in orange
        print(colored(message, 'light_red'))
    else:
        # Print message in default color
        print(message)


print_message('''---------------------Start Of session----------------------''', 'system_info')
print_message(current_date_time)

# object 1
print_message('''{current_date_time} deonstration of object XXX start''', 'system_info')

# create
print_message('''{current_date_time} going to create db cluster names "example"''', 'action')
clusterController.create('example')
print_message('''{current_date_time} cluster "example" created successfully''', 'system_info')
print_message(total_duration)

# delete
print_message('''{current_date_time} going to delete db cluster "example"''', 'action')
clusterController.delete('example')
print_message('{current_date_time} verify db cluster "example" deleted by checking if it exist', 'action')
clusterTest.verify_deletion('example')
print_message('''{current_date_time} cluster "example" deleted successfully''', 'system_info')
print_message(total_duration, 'system_info')

print_message('''{current_date_time} deonstration of object XXX ended successfully''', 'system_info')
print_message(total_duration, 'system_info')

print_message(current_date_time, 'system_info')
print_message('''---------------------End Of session----------------------''', 'system_info')
print_message(total_duration, 'system_info')
