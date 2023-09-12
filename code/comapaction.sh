#!/bin/bash

# Define Hive CLI command and options
HIVE_CMD="hive -e"

# Define the Hive query
CONSOLIDATE_QUERY="INSERT INTO consolidated_control_table_temp SELECT * FROM control_table;"

# Create a backup table with the same schema as control_table (if it doesn't exist)
$HIVE_CMD "CREATE TABLE IF NOT EXISTS backup_control_table AS SELECT * FROM control_table;"

# Run the consolidation query
$HIVE_CMD "$CONSOLIDATE_QUERY"

# Check for errors in the previous command
if [ $? -eq 0 ]; then
    # If successful, continue with the next steps

    # Optionally, you can perform additional checks or operations here

    # Replace the original table with the consolidated table
    $HIVE_CMD "ALTER TABLE control_table RENAME TO backup_control_table;"
    $HIVE_CMD "ALTER TABLE consolidated_control_table_temp RENAME TO control_table;"

    echo "Successfully consolidated and replaced the table."
else
    # If an error occurred, log it (consider sending alerts) and roll back
    echo "Error occurred during consolidation. Rolling back."
    $HIVE_CMD "ALTER TABLE control_table RENAME TO control_table_failed;"

    # Optionally, you can attempt to recover by restoring the backup table
    $HIVE_CMD "ALTER TABLE backup_control_table RENAME TO control_table;"
fi

# You can add more queries or actions here as needed

# Optionally, send a notification or log the script's completion
echo "Daily job completed."

# Exit the script
exit 0
