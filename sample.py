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

