from datetime import datetime
import time
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from Storage.NEW_KT_Storage.Controller.LockController import LockController

# demonstrate lock functionality
print('''---------------------Start Of session----------------------''')
start_time_session = datetime.now()
print(f"\033[33m{start_time_session} demonstration of object: Lock start\033[0m")

start_time = datetime.now()
lock_controller = LockController()

# create lock
print()

bucket_key = "myBucket"
object_key = "myLastObj"
lock_mode = "write"  
amount = 10
unit = "d"  
lock_id = f"{bucket_key}.{object_key}"

print(f"\033[33m{datetime.now()} start creating lock 'id: {lock_id}'\033[0m")

lock_controller.create_lock(bucket_key=bucket_key, object_key=object_key, lock_mode=lock_mode, amount=amount, unit=unit)
print(f"\033[32m{datetime.now()} lock: '{lock_id}' created successfully\033[0m")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"\033[33mtotal duration of create lock is: '{total_duration}'\033[0m")
print()
time.sleep(20)

# get lock
print()
start_time = datetime.now()
print(f"\033[33m{start_time} start getting lock '{lock_id}'\033[0m")
lock_example = lock_controller.get_lock(lock_id)
print(f"\033[32m{datetime.now()} verify lock 'id: {lock_example.lock_id}, "
    f"bucket_key: {lock_example.bucket_key}, object_key: {lock_example.object_key}, "
    f"retain_until = {lock_example.retain_until}, lock_mode: {lock_example.lock_mode}'\033[0m")

end_time = datetime.now()
total_duration = end_time - start_time
print(f"\033[33mtotal duration of get lock is: '{total_duration}'\033[0m")
print()
time.sleep(20)

# check if object is updatable
print()
start_time = datetime.now()
print(f"\033[33m{start_time} start checking if object '{object_key}' in bucket '{bucket_key}' is updatable\033[0m")
is_updatable = lock_controller.is_object_updatable(bucket_key=bucket_key, object_key=object_key)
print(f"\033[32m{datetime.now()} object '{object_key}' in bucket '{bucket_key}' is updatable: {is_updatable}\033[0m")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"\033[33mtotal duration of updatable check is: '{total_duration}'\033[0m")
print()
time.sleep(20)

# check if object is deletable
print()
start_time = datetime.now()
print(f"\033[33m{start_time} start checking if object '{object_key}' in bucket '{bucket_key}' is deletable\033[0m")
is_deletable = lock_controller.is_object_deletable(bucket_key=bucket_key, object_key=object_key)
print(f"\033[32m{datetime.now()} object '{object_key}' in bucket '{bucket_key}' is deletable: {is_deletable}\033[0m")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"\033[33mtotal duration of deletable check is: '{total_duration}'\033[0m")
print()
time.sleep(20)

# delete lock
print()
start_time = datetime.now()
print(f"\033[33m{start_time} start deleting lock '{lock_id}'\033[0m")
lock_controller.delete_lock(lock_id)
print(f"\033[32m{datetime.now()} lock: '{lock_id}' deleted successfully\033[0m")

end_time = datetime.now()
total_duration = end_time - start_time
print(f"\033[33mtotal duration of delete lock is: '{total_duration}'\033[0m")

# test deletion of lock by attempting to get it
print()
print(f"{datetime.now()} verify lock '{lock_id}' was deleted by trying to get it")
try:
    lock_controller.get_lock(lock_id)
except Exception as e:
    print(f"\033[31mError as expected: {e}\033[0m")
print()

# end of session
end_time_session = datetime.now()
print(f"\033[33m{end_time_session} demonstration of object lock ended successfully\033[0m")
total_duration_session = end_time_session - start_time_session
print(f"\033[33mthe total duration of session lock is: '{total_duration_session}'\033[0m")
print('''---------------------End Of session----------------------''')
