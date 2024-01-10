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
from unittest.mock import patch, call
from your_module import HiveDataProcessor  # Replace 'your_module' with the actual module name

class TestHiveDataProcessor(unittest.TestCase):
    def setUp(self):
        # Create an instance of HiveDataProcessor for testing
        self.hive_processor = HiveDataProcessor(max_workers=5, batch_size=5)

    @patch('your_module.HiveDataProcessor.execute_single_insert')
    @patch('queue.Queue.put')
    @patch('queue.Queue.get')
    def test_collect_and_execute_insert_with_values(self, mock_get, mock_put, mock_execute_single_insert):
        # Set up the mock behavior for Queue.put and Queue.get
        mock_get.side_effect = lambda: "SELECT query"  # Mock get to return a SELECT query
        mock_put.side_effect = lambda x: None  # Mock put to do nothing

        # Call the method to collect and execute insert with values
        self.hive_processor.collect_and_execute_insert()

        # Assert that the execute_single_insert method was called with the expected argument
        mock_execute_single_insert.assert_called_once_with("SELECT query")

        # Assert that Queue.put was called once
        mock_put.assert_called_once()

        # Assert that Queue.get was called until the queue is empty
        self.assertEqual(mock_get.call_count, 1)

    @patch('your_module.HiveDataProcessor.execute_single_insert')
    @patch('queue.Queue.put')
    @patch('queue.Queue.get')
    def test_collect_and_execute_insert_with_no_values(self, mock_get, mock_put, mock_execute_single_insert):
        # Set up the mock behavior for Queue.put and Queue.get
        mock_get.side_effect = lambda: None  # Mock get to return None (empty queue)
        mock_put.side_effect = lambda x: None  # Mock put to do nothing

        # Call the method to collect and execute insert with no values
        self.hive_processor.collect_and_execute_insert()

        # Assert that the execute_single_insert method was not called
        mock_execute_single_insert.assert_not_called()

        # Assert that Queue.put was not called
        mock_put.assert_not_called()

        # Assert that Queue.get was not called
        mock_get.assert_not_called()

if __name__ == "__main__":
    unittest.main()
    
    
   import unittest
from unittest.mock import patch
from your_module import YourClass  # Replace 'your_module' and 'YourClass' with your actual module and class names

class TestYourClass(unittest.TestCase):

    def setUp(self):
        # You can create an instance of your class and set up any necessary resources for testing
        self.your_instance = YourClass()

    def tearDown(self):
        # Clean up any resources created during testing
        pass

    @patch.object(YourClass, 'execute_single_insert')
    def test_collect_and_execute_insert_with_values(self, mock_execute):
        # Test the method when there are values in the queue
        # For this test, you might want to mock the value_queue to simulate values being present
        # and mock the execute_single_insert method to check if it's called with the expected argument

        # Example mock using unittest.mock.patch
        with patch.object(self.your_instance, 'value_queue') as mock_queue:
            # Set up the mock queue to return a value
            mock_queue.empty.side_effect = [False, True]  # Returns False on the first call, then True

            # Set up the mock queue to return a value
            mock_queue.get.return_value = "SELECT * FROM table1"

            # Call the method to be tested
            self.your_instance.collect_and_execute_insert()

            # Assertions to check if the methods were called as expected
            mock_queue.get.assert_called_once()  # Ensure get method is called
            mock_execute.assert_called_once_with("SELECT * FROM table1")  # Ensure execute_single_insert is called with the expected argument

    @patch.object(YourClass, 'execute_single_insert')
    def test_collect_and_execute_insert_with_no_values(self, mock_execute):
        # Test the method when there are no values in the queue
        # In this case, you might want to assert that execute_single_insert is not called

        with patch.object(self.your_instance, 'value_queue') as mock_queue:
            # Set up the mock queue to return an empty queue
            mock_queue.empty.return_value = True

            # Call the method to be tested
            self.your_instance.collect_and_execute_insert()

            # Assertions to check if the methods were called as expected
            mock_queue.get.assert_not_called()  # Ensure get method is not called
            mock_execute.assert_not_called()  # Ensure execute_single_insert is not called

if __name__ == '__main__':
    unittest.main()

