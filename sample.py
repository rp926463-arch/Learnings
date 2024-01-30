import unittest
from unittest.mock import patch, MagicMock
from your_module import YourClass  # Replace 'your_module' with your actual module name

class TestYourClass(unittest.TestCase):
    def setUp(self):
        # Create an instance of YourClass
        self.tc = YourClass()

    def test_remove_line_pyS(self):
        # Mocking dataRDD
        mock_data_rdd = MagicMock()

        # Mock zipWithIndex method
        with patch.object(mock_data_rdd, 'zipWithIndex') as mock_zip_with_index:
            # Mock filter method
            with patch.object(mock_zip_with_index.return_value, 'filter') as mock_filter:
                # Mock map method
                with patch.object(mock_filter.return_value, 'map') as mock_map:
                    # Call the method you want to test
                    with patch.object(self.tc.mocked_spark_session.sparkContext, 'parallelize', return_value=mock_data_rdd) as mock_parallelize:
                        result = self.tc.remove_line_pyS(mock_data_rdd, "HEAD", 2, 5)

                        # Assert that SparkContext.parallelize method was called
                        mock_parallelize.assert_called_once_with([], 1)

                        # Assert that zipWithIndex was called on the dataRDD
                        mock_zip_with_index.assert_called_once_with()

                        # Assert that filter and map methods were called appropriately
                        mock_filter.assert_called_once_with(lambdax=mock.ANY)
                        mock_map.assert_called_once_with(lambda row: row[0])

                        # Add additional assertions based on the behavior of remove_line_pyS
                        # For example, you can check the result or other conditions

    # Add more test cases as needed

if __name__ == '__main__':
    unittest.main()

    # Add more test cases as needed

if __name__ == '__main__':
    unittest.main()

import unittest
from unittest.mock import patch, MagicMock
from your_module import YourClass  # Replace 'your_module' with your actual module name

class TestYourClass(unittest.TestCase):
    @patch('your_module.SparkContext')
    @patch('your_module.YourClass.remove_line_pyS')
    @patch('your_module.pyspark.sql.SparkSession.builder.getOrCreate')
    def test_readTxtFile(self, mock_get_or_create, mock_remove_line_pyS, mock_SparkContext):
        # Mock SparkContext
        mock_spark_context = MagicMock()
        mock_SparkContext.return_value = mock_spark_context

        # Mock SparkSession
        mock_spark_session = MagicMock()
        mock_get_or_create.return_value = mock_spark_session

        # Mock remove_line_pyS method
        mock_remove_line_pyS.side_effect = lambda rdd, s, st, n: rdd

        # Create an instance of YourClass with the mock SparkSession
        your_instance = YourClass(spark=None)

        # Call the method you want to test
        filepath = "path/to/your/file.txt"
        delimiter = ","
        header_cnt = 2
        footer_cnt = 1
        header_str = ["col1", "col2", "col3"]

        with patch.object(mock_spark_context, 'textFile') as mock_text_file:
            mock_text_file.return_value = ["line1", "line2", "line3"]  # Mocked textFile result

            result_df = your_instance.readTxtFile(filepath, delimiter, header_cnt, footer_cnt, header_str)

            # Assert that textFile method was called with the correct argument
            mock_text_file.assert_called_once_with(filepath)

            # Assert that remove_line_pyS was called twice
            mock_remove_line_pyS.assert_has_calls([
                unittest.mock.call(["line1", "line2", "line3"], 'HEAD', "", header_cnt),
                unittest.mock.call(["line1", "line2", "line3"], 'TAIL', "", footer_cnt)
            ])

            # Additional assertions on the result_df if needed
            # For example, you can check the schema, count, etc.
            self.assertEqual(result_df.count(), 3)
            self.assertEqual(result_df.columns, ["col1", "col2", "col3"])

if __name__ == '__main__':
    unittest.main()


import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from your_module import Generator  # Replace 'your_module' with your actual module name

class TestGenerator(unittest.TestCase):

    @patch('your_module.SparkSession.builder.getOrCreate')
    def test_remove_line_pyS_head(self, mock_getOrCreate):
        # Create a mock SparkContext
        mock_spark_context = MagicMock()
        mock_getOrCreate.return_value.sparkContext = mock_spark_context

        # Create a mock SparkSession
        mock_spark_session = MagicMock()
        mock_spark_session.builder.getOrCreate.return_value = mock_spark_session

        # Create a mock Generator instance
        generator = Generator(mock_spark_session)

        dataRDD = mock_spark_context.parallelize(["line1", "line2", "line3", "line4", "line5"])
        section = "HEAD"
        start_at = ""
        num_of_lines = 2

        resultRDD = generator.remove_line_pyS(dataRDD, section, start_at, num_of_lines).collect()

        # Assert that the resultRDD contains the expected lines
        self.assertEqual(resultRDD, ["line3", "line4", "line5"])

    @patch('your_module.SparkSession.builder.getOrCreate')
    def test_remove_line_pyS_tail(self, mock_getOrCreate):
        # Create a mock SparkContext
        mock_spark_context = MagicMock()
        mock_getOrCreate.return_value.sparkContext = mock_spark_context

        # Create a mock SparkSession
        mock_spark_session = MagicMock()
        mock_spark_session.builder.getOrCreate.return_value = mock_spark_session

        # Create a mock Generator instance
        generator = Generator(mock_spark_session)

        dataRDD = mock_spark_context.parallelize(["line1", "line2", "line3", "line4", "line5"])
        section = "TAIL"
        start_at = ""
        num_of_lines = 2

        resultRDD = generator.remove_line_pyS(dataRDD, section, start_at, num_of_lines).collect()

        # Assert that the resultRDD contains the expected lines
        self.assertEqual(resultRDD, ["line1", "line2", "line3"])

    def test_readTxtFile(self):
        # Create a mock Generator instance
        generator = Generator(spark=None)

        filepath = "path/to/your/file.txt"
        delimiter = ","
        header_cnt = 2
        footer_cnt = 1
        header_str = ["col1", "col2", "col3"]

        # Mock the necessary methods in readTxtFile
        with patch.object(generator, 'remove_line_pyS') as mock_remove_line_pyS:
            mock_remove_line_pyS.return_value = ["line1", "line2", "line3"]
            result_df = generator.readTxtFile(filepath, delimiter, header_cnt, footer_cnt, header_str)

        # Perform assertions on the result_df as needed
        # For example, you can check the schema, count, etc.
        self.assertEqual(result_df.count(), 3)
        self.assertEqual(result_df.columns, ["col1", "col2", "col3"])

if __name__ == '__main__':
    unittest.main()





@patch('your_module.spark.read.format')
@patch('your_module.spark.read.load')
def test_spark_read_source_db(self, mock_logger, mock_format, mock_load):
    # Set up the input parameters
    spark = MagicMock()
    source_format = 'parquet'
    source_query = 'SELECT * FROM table'
    source_options = {'option1': 'value1', 'option2': 'value2'}
    datastore_type = 'source_db'
    read_opt = 'dbTable'

    # Mock the logger
    mock_logger_instance = MagicMock()
    mock_logger.return_value = mock_logger_instance

    # Call the method under test
    result = DbUtils.spark_read_source_db(
        spark, source_format, source_query, source_options, datastore_type, read_opt
    )

    # Assertions
    mock_logger_instance.info.assert_called_with(
        f'Reading data from {datastore_type}, with options\n : {source_options}'
    )

    mock_format.assert_called_with(source_format)
    mock_format.return_value.options.assert_called_with(**source_options)
    mock_format.return_value.option.assert_called_with(read_opt, source_query)
    mock_load.assert_called()

import pyodbc

import pyodbc

def update_teradata_table(connection_string, table_name, update_column, new_value, condition_column, condition_value):
    connection = None
    cursor = None

    try:
        # Establish a connection to the Teradata database
        connection = pyodbc.connect(connection_string)

        # Create a cursor
        cursor = connection.cursor()

        # Build and execute the UPDATE SQL statement
        update_query = f"UPDATE {table_name} SET {update_column} = ? WHERE {condition_column} = ?"
        cursor.execute(update_query, new_value, condition_value)

        # Commit the transaction
        connection.commit()

        # Check if the update was successful
        if cursor.rowcount > 0:
            print(f"Update successful. {cursor.rowcount} rows affected.")
        else:
            print("Update did not affect any rows.")

    except Exception as e:
        print(f"Error updating Teradata table: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Example usage:
# Specify your Teradata connection details
teradata_connection_string = "DRIVER={Teradata};DBCNAME=your_host;UID=your_username;PWD=your_password"

# Specify the table and update details
table_name = "your_table"
update_column = "column_to_update"
new_value = "new_value"
condition_column = "condition_column"
condition_value = "condition_value"

# Perform the update
update_teradata_table(teradata_connection_string, table_name, update_column, new_value, condition_column, condition_value)


# Example usage:
# Specify your Teradata connection details
teradata_connection_string = "DRIVER={Teradata};DBCNAME=your_host;UID=your_username;PWD=your_password"

# Specify the table and update details
table_name = "your_table"
update_column = "column_to_update"
new_value = "new_value"
condition_column = "condition_column"
condition_value = "condition_value"

# Perform the update
update_teradata_table(teradata_connection_string, table_name, update_column, new_value, condition_column, condition_value)












from pyspark.sql import SparkSession

def get_spark_connection(app_name):
    # Your implementation to create and return a Spark session
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def read_data_into_dataframe(spark, source_system, query, source_options):
    # Your implementation to read data into a DataFrame based on source_system and query
    df = spark.read.format(source_system).options(**source_options).option("query", query).load()
    return df

def write_data_to_target(df, target_system, target_table, target_options):
    # Your implementation to write data from DataFrame to a database based on target_system and target_table
    df.write.format(target_system).options(**target_options).option("dbtable", target_table).mode("overwrite").save()

def process_json_config(json_config):
    for product, queries in json_config.items():
        print(f"Processing {product}...")

        for query_info in queries:
            app_name = query_info["app_name"]
            source_system = query_info["source_system"]
            query = query_info["query"]
            target_system = query_info.get("target_system", source_system)  # Use source system as default target system

            print(f"Executing query for {app_name}...")

            # Create Spark session
            spark = get_spark_connection(app_name)

            # Read data into DataFrame
            source_options = query_info.get("source_options", {})
            df = read_data_into_dataframe(spark, source_system, query, source_options)

            # Your logic to process the DataFrame as needed

            # Write data to target
            target_table = query_info.get("target_table", f"{product}_{app_name}")
            target_options = query_info.get("target_options", {})
            write_data_to_target(df, target_system, target_table, target_options)

            # Stop Spark session
            spark.stop()

if __name__ == "__main__":
    # Your JSON configuration
    json_config = {
        "Product1": [
            {"app_name": "spark_job_id_dl1", "source_system": "parquet", "query": "select * from table1", "target_system": "parquet"},
            {"app_name": "spark_job_id_dl2", "source_system": "parquet", "query": "select * from table1", "target_system": "csv"},
        ],
        "Product2": [
            {"app_name": "spark_job_id2", "source_system": "csv", "query": "select * from table1", "target_system": "parquet"},
        ],
    }

    # Process JSON configuration
    process_json_config(json_config)





def insert inte_control_tol(self, job id, status, source container, source table, max date, filtr):
    if max date is None:
        query = f"insert into dl rbg_work.dif_status select {job_id}, CURRENT DATE, {status}"
    elif filtr and job_id in self.control_dt_map:
        filtr = f"where {max_date} > {self.control dt_map[job_id]}"
        query = f"insert into dl_rbg_work.dif_status select {job_id}, max({max_date}), {status} from {source_container}.{source_table} {filtr}"
    else:
        query = f"insert into dl_rbg_work.dif_status select '{joo_id}', max({max date}), '{status}' from {source container}.{source_table} {filtr}"
        
    self.logger.info(f"Control table insert query for JOB={job_id): (query)")
    start time datetime.now()
    rtn_cde os.system(fhive -e "(query)"")
    end_time = datetime.now()
    if not rtn_cde:
        self.logger.info("Insertion to control table Successful for job {}".format(job_id))
        self.logger.info(f'Insertion to control table for 308=job_id) took end_time start time}")
    else:
        self.logger.error("Insertion to control table Failed for job {}".format(job_id))

from datetime import datetime
import time

def insert_into_control_table(self, job_id, status, source_container, source_table, max_date, filtr):
    max_retries = 3
    retry_delay_seconds = 5
    
    for attempt in range(max_retries):
        if max_date is None:
            query = f"insert into dl_rbg_work.dif_status select {job_id}, CURRENT DATE, {status}"
        elif filtr and job_id in self.control_dt_map:
            filtr = f"where {max_date} > {self.control_dt_map[job_id]}"
            query = f"insert into dl_rbg_work.dif_status select {job_id}, max({max_date}), {status} from {source_container}.{source_table} {filtr}"
        else:
            query = f"insert into dl_rbg_work.dif_status select '{job_id}', max({max_date}), '{status}' from {source_container}.{source_table} {filtr}"
        
        self.logger.info(f"Control table insert query for JOB={job_id}: {query}")
        start_time = datetime.now()
        rtn_code = os.system(f"hive -e \"{query}\"")
        end_time = datetime.now()
        
        if not rtn_code:
            self.logger.info(f"Insertion to control table Successful for job {job_id}")
            self.logger.info(f"Insertion to control table for job {job_id} took {end_time - start_time}")
            break  # Exit the loop if insertion is successful
        else:
            self.logger.error(f"Insertion to control table Failed for job {job_id}")
            self.logger.error(f"Attempt {attempt + 1}/{max_retries}")
            self.logger.info(f"Retrying in {retry_delay_seconds} seconds...")
            time.sleep(retry_delay_seconds)
                

    if rtn_code:
        self.logger.error(f"Failed to insert into control table after {max_retries} attempts for job {job_id}")


'''
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
'''
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
