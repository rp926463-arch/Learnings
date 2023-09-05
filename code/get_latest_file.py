import glob
import os

pattern = r'C:\Users\rosha\Downloads\GIT_Repos\Learnings\code\data_files\LLM_20230905_*.dat'

# Use glob to find files matching the pattern
files = glob.glob(pattern)

# Sort the files by modification time (latest first)
files.sort(key=os.path.getmtime, reverse=True)

if files:
    latest_file = files[0]
    print("Latest file:", latest_file)
else:
    print("No matching files found.")




    
    

import glob

pattern = '/path/to/directory/file_*.dat'

# Use glob to find files matching the pattern
files = glob.glob(pattern)

# Initialize variables to keep track of the latest file and its timestamp
latest_file = None
latest_timestamp = None

# Iterate through the matching files and find the one with the latest timestamp
for file_path in files:
    # Extract the timestamp from the filename
    file_name = os.path.basename(file_path)
    timestamp_str = file_name.split('_')[1]  # Assumes the timestamp is the second part of the filename
    file_timestamp = int(timestamp_str)  # Convert the timestamp to an integer for comparison
    
    # Compare the timestamps
    if latest_timestamp is None or file_timestamp > latest_timestamp:
        latest_timestamp = file_timestamp
        latest_file = file_path

if latest_file:
    print("Latest file:", latest_file)
else:
    print("No matching files found.")

