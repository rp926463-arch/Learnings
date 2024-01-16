from queue import Queue

class YourClass:
    def __init__(self):
        self.value_queue = Queue()
        self.value_queue.put(('dummy_job1', 'NULL', 'SUCCESS'))
        self.value_queue.put(('dummy_job2', '2023-10-01', 'SUCCESS'))

    def process_queue(self):
        formatted_values = []
        values_str_template = "({})"
        
        while not self.value_queue.empty():
            entry = self.value_queue.get()
            formatted_entry = ", ".join(["NULL" if val == 'NULL' else f"'{val}'" for val in entry])
            formatted_values.append(values_str_template.format(formatted_entry))

        values_str = ",\n".join(formatted_values)
        insert_template = f"INSERT INTO dl_rbg_work.dif_status VALUES {values_str};"

        return insert_template

# Example usage
your_instance = YourClass()
result = your_instance.process_queue()
print(result)

print("Expected output:", result)

#https://www.snowflake.com/blog/managing-snowflakes-compute-resources/

#https://www.linkedin.com/pulse/snowflake-databricks-bigquery-dataproc-redshift-emr-lakshmanan

'''
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

class HiveDataProcessor:
    def __init__(self, max_workers=5, batch_size=5):
        self.value_queue = Queue()
        self.insert_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.batch_size = batch_size

    def execute_single_insert(self, values):
        insert_statement = f"INSERT INTO TABLE dl_rbg_work.dif_status VALUES {', '.join(values)}"
        # Replace the following line with your Hive execution logic
        print(f"Executing single insert: {insert_statement}")

    def add_values_to_queue(self, values):
        with self.insert_lock:
            self.value_queue.put(values)

    def process_data_parallel(self):
        for i in range(10):
            values = f"({i}, 'data{i}')"
            self.executor.submit(self.add_values_to_queue, values)

        # Wait for all tasks to complete
        self.executor.shutdown(wait=True)

    def collect_and_execute_insert(self):
        """
        Collect SELECT queries from the value_queue and execute a single insert statement.

        Note:
        - This method collects all SELECT queries from the value_queue and combines them into a single
          insert statement using UNION ALL. The combined statement is then executed.
        - If the value_queue is empty, a message is printed indicating that there are no values to insert.
        """
        all_values = []
        while not self.value_queue.empty():
            all_values.append(self.value_queue.get())

        self.execute_single_insert(all_values)

if __name__ == "__main__":
    hive_processor = HiveDataProcessor(max_workers=5, batch_size=5)
    hive_processor.process_data_parallel()
    hive_processor.collect_and_execute_insert()








import time

def insert_with_retry(batch_data):
    max_retries = 3
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            # Perform the Hive insert operation for the batch_data
            # Example: hive_query = "INSERT INTO TABLE dl_rbg_work.dif_status VALUES ..."
            # Execute the query here
            # ...
            print("Insert successful!")
            break  # Break out of the loop if the insert is successful
        except Exception as e:
            print(f"Error during insert attempt {attempt + 1}: {str(e)}")
            time.sleep(retry_delay * (attempt + 1))  # Increase delay for each retry
    else:
        print("Max retries reached. Insert failed.")

# Example usage
batch_data_to_insert = [...]  # Your data for the batch
insert_with_retry(batch_data_to_insert)







import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

class HiveDataProcessor:
    def __init__(self, max_workers=5, batch_size=5):
        self.value_queue = Queue()
        self.insert_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.batch_size = batch_size

    def execute_single_insert(self, combined_select_queries):
        """
        Execute a single insert statement based on combined SELECT queries.

        Parameters:
        - combined_select_queries (str): A string containing SELECT queries combined using UNION ALL.
        
        """
        insert_statement = f"INSERT INTO TABLE dl_rbg_work.dif_status {combined_select_queries}"
        # Replace the following line with your Hive execution logic
        print(f"Executing single insert: {insert_statement}")

    def add_select_query_to_queue(self, select_query):
        with self.insert_lock:
            self.value_queue.put(select_query)

    def process_data_parallel(self):
        for i in range(10):
            # Generate your select query dynamically based on your requirements
            select_query = f"SELECT '{i}', MAX(dte), 'SUCCESS' FROM dl_rbg_rewf.source_table"
            self.executor.submit(self.add_select_query_to_queue, select_query)

        # Wait for all tasks to complete
        self.executor.shutdown(wait=True)

    def collect_and_execute_insert(self):
        all_select_queries = []
        while not self.value_queue.empty():
            all_select_queries.append(self.value_queue.get())

        # Combine all select queries into a single insert statement using UNION ALL
        combined_select_queries = " UNION ALL ".join(all_select_queries)
        self.execute_single_insert(combined_select_queries)

if __name__ == "__main__":
    hive_processor = HiveDataProcessor(max_workers=5, batch_size=5)
    hive_processor.process_data_parallel()
    hive_processor.collect_and_execute_insert()




import subprocess
import time

def insert_with_retry(batch_data):
    max_retries = 3
    retry_delay = 5  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            result = subprocess.run(["your_command_here"], shell=True, check=True, text=True)
            print("Insert successful!")
            break  # Break out of the loop if the insert is successful
        except subprocess.CalledProcessError as e:
            print(f"Error during insert attempt {attempt}: {e}")
            time.sleep(retry_delay * attempt)  # Increase delay for each retry
    else:
        print("Max retries reached. Insert failed.")

# Replace "your_command_here" with your actual insert query




import os
import time

def insert_with_retry(batch_data):
    max_retries = 3
    retry_delay = 5  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            rtn_code = os.system("your_command_here")
            if rtn_code == 0:
                print("Insert successful!")
                break  # Break out of the loop if the insert is successful
            else:
                print(f"Non-zero return code during insert attempt {attempt}: {rtn_code}")
        except Exception as e:
            print(f"Error during insert attempt {attempt}: {str(e)}")
        
        time.sleep(retry_delay * attempt)  # Increase delay for each retry
    else:
        print("Max retries reached. Insert failed.")

# Replace "your_command_here" with your actual insert query



import unittest
from unittest.mock import patch, Mock
from your_module import insert_with_retry  # Replace 'your_module' with your actual module name

class TestInsertWithRetry(unittest.TestCase):

    @patch('your_module.subprocess.run')
    @patch('time.sleep')
    def test_successful_insert(self, mock_sleep, mock_subprocess_run):
        # Test the case where the insert is successful on the first attempt

        # Set up the mock subprocess.run to return a successful result
        mock_subprocess_run.return_value = Mock(returncode=0, stdout="Insert successful!")

        # Call the method to be tested
        insert_with_retry("batch_data")

        # Assertions to check if the methods were called as expected
        mock_subprocess_run.assert_called_once_with(["your_command_here"], shell=True, check=True, text=True)
        mock_sleep.assert_not_called()  # Ensure sleep is not called since the insert is successful

    @patch('your_module.subprocess.run')
    @patch('time.sleep')
    def test_failed_insert_with_retry(self, mock_sleep, mock_subprocess_run):
        # Test the case where the insert fails multiple times before succeeding

        # Set up the mock subprocess.run to raise an exception on the first two calls, then succeed on the third call
        mock_subprocess_run.side_effect = [
            subprocess.CalledProcessError(returncode=1, cmd=["your_command_here"]),
            subprocess.CalledProcessError(returncode=2, cmd=["your_command_here"]),
            Mock(returncode=0, stdout="Insert successful!")
        ]

        # Call the method to be tested
        insert_with_retry("batch_data")

        # Assertions to check if the methods were called as expected
        mock_subprocess_run.assert_called_with(["your_command_here"], shell=True, check=True, text=True)
        self.assertEqual(mock_subprocess_run.call_count, 3)  # Ensure subprocess.run is called three times
        self.assertEqual(mock_sleep.call_count, 2)  # Ensure sleep is called twice (for the second and third attempts)

    @patch('your_module.subprocess.run')
    @patch('time.sleep')
    def test_failed_insert_max_retries_reached(self, mock_sleep, mock_subprocess_run):
        # Test the case where the insert fails on all attempts, and max retries are reached

        # Set up the mock subprocess.run to raise an exception on all calls
        mock_subprocess_run.side_effect = subprocess.CalledProcessError(returncode=1, cmd=["your_command_here"])

        # Call the method to be tested
        insert_with_retry("batch_data")

        # Assertions to check if the methods were called as expected
        mock_subprocess_run.assert_called_with(["your_command_here"], shell=True, check=True, text=True)
        self.assertEqual(mock_subprocess_run.call_count, 3)  # Ensure subprocess.run is called three times
        self.assertEqual(mock_sleep.call_count, 2)  # Ensure sleep is called twice (for the second and third attempts)

if __name__ == '__main__':
    unittest.main()


 mock_file = MagicMock(spec=open)
        mock_file.__enter__.return_value.__iter__.return_value = iter(["Line 1", "Line 2", "Line 3"])
        mock_open.return_value = mock_file
https://superfastpython.com/threadpoolexecutor-thread-safe/

echo -e "Subject: Your Subject\nContent-Type: text/html\n\n<html><body style='color:red;'>This is red text</body></html>" | mailx -s "Your Subject" recipient@example.com

output_lines = result.stdout.strip().split('\n')
        
        # Parsing the output into a tuple
        output_tuple = tuple(map(str.strip, output_lines[1].split(',')))

        return output_tuple
        
output_tuple = tuple(
    (value.strip() if value.strip().lower() != 'null' else None)
    for value in output_lines[1].split(',')
)


# Define the Hive table and columns
hive_table = "dl_rbg_work.dif_status"
columns = ["job_name", "date", "status"]

# Create the SQL insert statement
insert_template = "INSERT INTO {} ({}) VALUES {};"
columns_str = ", ".join(columns)
values_str_template = "('{}', '{}', '{}')"

# Build the VALUES part of the query using the queue entries
values_str = ",\n       ".join(values_str_template.format(*data_queue.get()) for _ in range(data_queue.qsize()))

# Construct the final insert statement
insert_statement = insert_template.format(hive_table, columns_str, values_str)

while not data_queue.empty():
    entry = data_queue.get()
    formatted_values.append(", ".join(["'{}'"] * len(entry)).format(*entry))

# Join the formatted values into a single string
values_str = ",\n       ".join(formatted_values





values_str_template = "({})"
formatted_values = []

# Dynamically construct the VALUES part using a while loop
while not data_queue.empty():
    entry = data_queue.get()
    formatted_values.append(values_str_template.format(", ".join(["'{}'"] * len(entry)).format(*entry)))

# Join the formatted values into a single string
values_str = ",\n       ".join(formatted_values)



_________________________________
import subprocess
import re

def run_hive_query(query):
    try:
        result = subprocess.run(["hive", "-e", query], shell=True, check=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Use a regular expression to extract values from the Hive query output
        match = re.search(r"(\w+)\s+([\d-]+)\s+(\w+)", result.stdout)
        
        if match:
            output_tuple = tuple(
                (value.strip() if value.strip().lower() != 'null' else None)
                for value in match.groups()
            )
            return output_tuple
        else:
            print("Error: Unable to parse Hive query output.")
            return None

    except subprocess.CalledProcessError as e:
        print(f"Error executing Hive query: {e}")
        return None

# Example query
hive_query = "select 'JOB_YD', max(job_date), 'SUCCESS' from dl_rbg_work.dif_status"

# Run the Hive query and get the output as a tuple
result_tuple = run_hive_query(hive_query)

# Print the result
print(result_tuple)


it is capturing 'HD2SF_GBT_TEXPRESS_CR_ACCOUNT_DL\t2023-09-29\tADHOC_SUCCESS\n' 
as
<re.Match object; span(0, 58), match='HD2SF_GBT_TEXPRESS_CR_ACCOUNT\t2023-09-29\tADH>


(.+?)\s+([\d-]+)\s+(.+)

pattern = r"(.+?)\s+([\d-]+)\s+(.+?)(?:\s|\n|$)"


still didnt work
pattern = r"([^\s]+)\s+([\d-]+)\s+([^\s]+)(?:\s|\n|$)"
text = 'HD2SF_GBT_TEXPRESS_CR_ACCOUNT_DL\t2023-09-29\tADHOC_SUCCESS\n'
match = re.search(pattern, text)
output : HD2SF_GBT_TEXPRESS_CR_ACCOUNT_DL\t2023-09-29\tADHOC_SUCCESS

<re.Match object; span=(0, 58), match='HD2SF_GBT_TEXPRESS_CR_ACCOUNT_DL\t2023-09-29\tADH>

Actual : <re.Match object; span=(0, 58), match='HD2SF_GBT_TEXPRESS_CR_ACCOUNT_DL\t2023-09-29\tADH>
required : <re.Match object; span=(0, 58), match='HD2SF_GBT_TEXPRESS_CR_ACCOUNT_DL\t2023-09-29\tADHOC_SUCCESS>




import re

def parse_text(input_text):
    pattern = r'\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*' if '|' in input_text else r'([^\t]+)\t([^\t]+)\t([^\t\n]+)'
    #data_lines = [line for line in input_text.split('\n') if '|' in line][-1]
    data_lines = [line for line in input_text.split('\n') if '|' in line]
    if data_lines:
        match = re.search(pattern, data_lines[-1])
        if match:
            return tuple(group.strip() for group in match.groups())
    #match = re.search(pattern, data_lines)
    #if match:
    #    return match.groups()


def parse_text(input_text):
    pattern = r'\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*' if '|' in input_text else r'([^\t]+)\t([^\t]+)\t([^\t\n]+)'
    data_lines = input_text
    if '|' in input_text:
        data_lines = [line for line in input_text.split('\n') if '|' in line][-1]
    match = re.search(pattern, data_lines)
    if match:
        return tuple(group.strip() for group in match.groups())
    

# Example usage with text1
text1 = 'HD2SF_GBT_TEXPRESS_CR_ACCOUNT_DL\t2023-09-29\tADHOC_SUCCESS\n'
result1 = parse_text(text1)
print("Result1:", result1)

# Example usage with text2
text2 = '+----------------\n|    _c0|    _c1|    _c2|\n+-------------------+\n|  HD2SF_GBT_TEXPRESS_CR_ACCOUNT_DL | 2023-09-29 | ADHOC_SUCCESS |\n'
result2 = parse_text(text2)
print("Result2:", result2)

result.returncode if hasattr(result, 'returncode') else None


Modify below code to get expected_output


self.value_queue = [('dummy_job1', 'NULL', 'SUCCESS'), ('dummy_job2', '2023-10-01', 'SUCCESS')]
formatted_values=[]
values_str_template="({})"
while not self.value_queue.empty():
    entry = self.value_queue.get()
    formatted_values.append(values_str_template.format(", ".join(["{}"] * len(entry)).format(entry)))

values_str = ", \n ".join(formatted_values)

insert_template = f"INSERT INTO dl_rbg_work.dif_status VALUES {values_str);"

print(insert_template)

expected_output = "INSERT INTO dl_rbg_work.dif_status VALUES ('dummy_job1', NULL, 'SUCCESS'), ('dummy_job2', '2023-10-01', 'SUCCESS')"
//
'''
