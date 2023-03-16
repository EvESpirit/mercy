import os
import zlib
import sqlite3
import threading
from datetime import datetime
from os import path
from sys import exit
import subprocess

# Elevate privileges
subprocess.call('runas /user:Administrator cmd')

# Check if reference.db and scanned.db exist and are larger than 100KB, and create a connection accordingly
if path.isfile("reference.db") and path.isfile("scanned.db") and path.getsize("reference.db") > 102400\
        and path.getsize("scanned.db") > 102400:
    exit("\nBoth files exist and contain valid data. Please run the validator instead. Exiting...")
else:
    if path.isfile("reference.db") and path.getsize("reference.db") > 102400:
        db_conn = sqlite3.connect("scanned.db")
    else:
        db_conn = sqlite3.connect("reference.db")

# Create a table to store the hashes
cursor = db_conn.cursor()
cursor.execute("DROP TABLE IF EXISTS crc32")
cursor.execute("CREATE TABLE crc32 (file_name TEXT, crc32 TEXT, hash_time TEXT)")

# Create a list to store the hashes
hashes = []

# Create a thread pool
thread_pool = []
lock = threading.Lock()


def extractFP(string):
    return string[string.find('\'') + 1:string.rfind('\'')]


# Create a function to hash the files
def hash_file(file_name):
    try:
        # Open the file
        with open(file_name, 'rb') as f:
            # Compute the zlib hash of the file
            file_hash = zlib.crc32(f.read()) & 0xffffffff

            # Get the current time
            current_time = datetime.now()

            # Acquire the lock
            lock.acquire()

            # Store the hash in the list
            hashes.append((file_name, file_hash, current_time))

            # Release the lock
            lock.release()
    except PermissionError as e:
        print(f"\nFile reading failed: '{extractFP(str(e))}'")


# Create a generator to iterate through the directory tree
def list_files():
    for root, dirs, files in os.walk("C:/Windows"):
        for file in files:
            yield os.path.join(root, file)


# Iterate through the directory tree
for file_name in list_files():
    # Create a thread for each file
    thread_pool.append(threading.Thread(target=hash_file, args=(file_name,)))

# Start all the threads
for thread in thread_pool:
    thread.start()

# Wait for all the threads to finish
for thread in thread_pool:
    thread.join()

# Store the hashes in the database
cursor.executemany("INSERT INTO crc32 VALUES (?,?,?)", hashes)

# Commit changes to the database
db_conn.commit()

# Close the database connection
db_conn.close()
