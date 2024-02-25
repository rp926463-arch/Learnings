#dictionary = {"key1": 1, "key2": 1, "key3": 7, "key4": 3, "key5": 4, "key6": 7}

#print(dictionary.items())

def find_unique_values1(dictionary):
    lst = []
    
    for k,v in dictionary.items():
        val = list(dictionary.values())
        val.remove(v)
        if v in val:
            pass
        else:
            lst.append(v)
    return lst

from collections import Counter
def find_unique_values2(dictionary):
    lst = []
    count = Counter(dictionary.values())
    print(count)
    for ele, val in count.items():
        if val==1:
            lst.append(ele)
    return lst
    

def get_max_value(dictionary):
    return max(dictionary.values())
    

def count_occurrences(lst):
    # Use Counter to count occurrences of elements in the list
    counts = Counter(lst)
    print(counts)
    # Convert Counter object to a dictionary
    occurrences_dict = dict(counts)
    return occurrences_dict
    
def countingValleys(steps, path):
    result = [0,0]
    cnt = 0
    
    for ele in list(path):
        if ele == 'D':
            if result[1] > 0:
                result[1] -= 1
            else:
                result[0] -= 1
        if ele == 'U':
            if result[0] < 0:
                result[0] += 1
            else:
                result[1] += 1
        print(result)
        if result == [0,0] and ele == 'U':
            cnt += 1
    return cnt
 
import math
def repeatedString(s, n):
    lst = list(s)
    slength = len(lst)
    #print(f'slength : {slength}')
    
    scountdict = dict(Counter(lst))
    #print(scountdict)
    acount = scountdict.get('a')
    
    if acount is not None:
        mfactor = math.floor(n/slength)
        #print(f'mfactor : {mfactor}')
        acount = mfactor * acount
        #print(f'acount : {acount}')
        
        if mfactor*slength < n:
            #print(f"mfactor*slength : {mfactor*slength}")
            tmp_dict = Counter(lst[:(n-(mfactor*slength))])
            #print(tmp_dict)
            #print(tmp_dict['a'])
            acount += tmp_dict['a']
    else:
        acount = 0
    
    return acount
    
#print(repeatedString('gfcaaaecbg', 547602))

def jumpingOnClouds(c):
    jumps = 0
    i = 0

    while i < len(c) - 1:
        if i + 2 < len(c) and c[i + 2] == 0:
            i += 2
        else:
            i += 1
        jumps += 1
    return jumps


def rotate_list(lst, k):
    if not lst:
        return lst
    k = k % len(lst)  # Ensure k is within the length of the list
    return lst[-k:] + lst[:-k]



import logging
import queue
import logging.handlers
from concurrent.futures import ThreadPoolExecutor
import threading
import time

def configure_logging():
    # Configure the root logger
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def worker_function(queue):
    logger = logging.getLogger()
    handler = logging.handlers.QueueHandler(queue)
    logger.addHandler(handler)

    # Log some messages with thread-specific information
    thread_info = f"Thread ID: {threading.get_ident()}, Thread Name: {threading.current_thread().name}"
    logger.info(f"This is a message from the worker. {thread_info}")
    
def new_function(val):
    logger = logging.getLogger()
    logger.info(f"This is a message from the worker. {threading.current_thread().name} for val : {val}")
    time.sleep(5)

def main():
    configure_logging()
    log_queue = queue.Queue()

    num_threads = 5  # Specify the number of threads
    val = [1,2,3,4,5]
    # Create a thread pool with the specified number of threads
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit multiple worker functions concurrently
        #executor.submit(worker_function, log_queue)
        for item in val:
            executor.submit(new_function, item)

    # Create a listener thread to handle logs from the queue
    listener_thread = logging.handlers.QueueListener(log_queue, logging.StreamHandler())
    listener_thread.start()

    # Wait for all threads to finish
    executor.shutdown()
    listener_thread.stop()




#2 Features
org_dict = {
    "Key1": "Value1",
    "Key2": "Value2",
    "Key3": "Value3"
}

class CaseInsensitiveDict(dict):    
    def get(self, key, default=None):
        if isinstance(key, str):
            for k, v in self.items():
                if k.lower() == key.lower():
                    return v
        return default

new_dict = CaseInsensitiveDict(org_dict)

#print(CaseInsensitiveDict(org_dict).get("Key1"), CaseInsensitiveDict(org_dict).get("key1"), CaseInsensitiveDict(org_dict).get("keys"))
#print(new_dict.get("Key1"), new_dict.get("key1"))

#1 Features
class SnowflakePythonConfig:
    def __init__(self, data, pkb=None, sf_database=None, sf_schema=None):
        self.user = data.get("sfUser")
        self.account = data.get("sfAccount")
        self.private_key = pkb
        self.warehouse = data.get("sfWarehouse")
        self.role = data.get("sfRole")
        self.database = sf_database
        self.schema = sf_schema
        self.write_opt = data.get("writeOption")
        self.read_opt = data.get("readOption")
        self.cert_scv_namespace = data.get("certScvNamespace")
        self.cert_scv_key = data.get("certScvKey")
        self.pass_scv_namespace = data.get("passScvNamespace")
        self.pass_scv_key = data.get("passScvKey")

    def update_key(self, pkb, database, schema):
        self.private_key = pkb
        self.database = database
        self.schema = schema

    def get_spark_options(self):
        relevant_options = {
            "sfUser": self.user,
            "sfAccount": self.account,
            "pem_private_key": self.private_key,
            "sfWarehouse": self.warehouse,
            "sfRole": self.role,
            "sfDatabase": self.database,
            "sfSchema": self.schema
        }
        return relevant_options

# Creating the SnowflakePythonConfig object
config_data = {
    "sfUser": "",
    "sfAccount": "",
    "pem_private_key": "",
    "sfWarehouse": "",
    "sfRole": "",
    "sfDatabase": "",
    "sfSchema": "",
    "readOption": "query",
    "writeOption": "dbTable",
    "certScvNamespace": "AIS/DUMMY/PROID",
    "certScvKey": "qbcd",
    "passScvNamespace": "AIS/DUMMY/PROID",
    "passScvKey": "zazx"
}

obj = SnowflakePythonConfig(config_data)

# Using the SnowflakePythonConfig object to get options for spark.read
options = obj.get_spark_options()

# Using options for spark.read
print(options)

print(obj.cert_scv_namespace)
print(obj.cert_scv_key)




#if __name__ == "__main__":
#    main()







# Example usage:
#my_list = [1, 2, 3, 4, 5]
#rotated_list = rotate_list(my_list, 2)
#print(rotated_list)  # Output: [4, 5, 1, 2, 3]



#print(jumpingOnClouds([0,1,0,1,0]))
#print(jumpingOnClouds([0,1,0,0,0,1,0]))
#print(jumpingOnClouds([0,1,0,0,0]))
#print(jumpingOnClouds([0,0,1,0,0,1,0]))
#print(jumpingOnClouds([0,1,0,0,0,0,1,0]))
#print(countingValleys(14, 'DDUUDDDUUUUDDU'))

#print(countingValleys(10, 'DUDDDUUDUU'))

# Example usage
#my_list = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]
#result_dict = count_occurrences(my_list)
#print(result_dict)




#print(find_unique_values2(dictionary))
#print(find_unique_values1(dictionary))
#print(get_max_value(dictionary))


#Numbers represent elevation. Find how much snow can be trapped in the sequence of numbers as input.