
import glob
import os
import re

pattern = r'C:\\Users\\rosha\\Downloads\\GIT_Repos\\Learnings\\dataFiles\\2*.txt'
#pattern = r'C:\\Users\\rosha\\Downloads\\GIT_Repos\\Learnings\\dataFiles\\TLEND*.txt'
#pattern = r'C:\\Users\\rosha\\Downloads\\GIT_Repos\\Learnings\\dataFiles\\FACILITY*.txt'
#pattern = r'C:\\Users\\rosha\\Downloads\\GIT_Repos\\Learnings\\dataFiles\\ELOC_LAL*.txt'
#pattern = r'C:\\Users\\rosha\\Downloads\\GIT_Repos\\Learnings\\dataFiles\\ELOC_LOL*.txt'

# Use glob to find files matching the pattern
files = glob.glob(pattern)
print(files)
# Initialize variables to keep track of the latest file and its timestamp
latest_file = None
latest_timestamp = None

timestamp_pattern = r'(\d+)|(\d+)\.txt'

# Iterate through the matching files and find the one with the latest timestamp
for file_path in files:
    # Extract the filename
    file_name = os.path.basename(file_path)

    # Use regular expression to extract timestamps
    matches = re.findall(timestamp_pattern, file_name)
    print(matches)
    #print(matches[-1][0])
    #matches = matches[-1][0]
    
    timestamp_str = next(filter(None, matches[-1]), None)

    print(timestamp_str)
    if timestamp_str:
        #print(timestamp_str)
        try:
            file_timestamp = int(timestamp_str)  # Convert the timestamp to an integer for comparison
            #print(file_timestamp)
            if latest_timestamp is None or file_timestamp > latest_timestamp:
                latest_timestamp = file_timestamp
                latest_file = file_path
        except ValueError:
            # Handle the case where the timestamp cannot be converted to an integer
            print(f"Skipping file {file_path} due to invalid timestamp: {timestamp_str}")

if latest_file:
    print("Latest file:", latest_file)
else:
    print("No matching files found.")