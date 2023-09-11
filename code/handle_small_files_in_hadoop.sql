CREATE TABLE consolidated_control_table_temp AS
SELECT *
FROM control_table
WHERE 1=0; -- This creates an empty table with the same schema as control_table

CREATE TABLE consolidated_control_table_temp
STORED AS ORC

CREATE TABLE my_partitioned_table
CLUSTERED BY (bucketed_column INT)
INTO 32 BUCKETS
STORED AS ORC;


#!/bin/bash

# Start a Hive session
hive -e "START TRANSACTION;"

# Perform the consolidation into the temporary table(Test both ways below pickup which consolidate files)
hive -e "INSERT INTO consolidated_control_table_temp SELECT * FROM control_table;"

hive -e "INSERT OVERWRITE TABLE consolidated_control_table SELECT column1, column2 FROM control_table;"

# Check for errors in the previous command
if [ $? -eq 0 ]; then
    # If successful, continue with the next steps

    # Optionally, you can perform additional checks or operations here

    # Replace the original table with the consolidated table
    hive -e "ALTER TABLE control_table RENAME TO backup_control_table;"
    hive -e "ALTER TABLE consolidated_control_table_temp RENAME TO control_table;"

    # Commit the transaction
    hive -e "COMMIT;"
else
    # If an error occurred, roll back the transaction
    hive -e "ROLLBACK;"
fi
