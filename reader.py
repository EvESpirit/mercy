import sqlite3
import multiprocessing
from os import path, system, stat
from sys import exit

# Check if reference.db and scanned.db exist
if not path.isfile('reference.db')\
        or not path.isfile('scanned.db')\
        or stat('reference.db').st_size < 16384\
        or stat('scanned.db').st_size < 16384:
    print("\nThe reference or scanned file databases are missing or too small to contain useful information. Would "
          "you like to run scanner.py to either generate a reference copy or run a scan? (y/Y or any key to "
          "exit): ")
    answer = input()
    if answer.lower() == 'y' or 'Y':
        system('python scanner.py')

    else:
        exit()
c1 = path.exists("reference.db")
c2 = path.exists("scanned.db")
c3 = stat('reference.db').st_size > 102400
c4 = stat('scanned.db').st_size > 102400

# Check if reference.db and scanned.db exist and are larger than 100KB
if c1 and c2 and c3 and c4:
    # Connect to both databases
    db1 = sqlite3.connect('reference.db')
    db2 = sqlite3.connect('scanned.db')
    # Create a cursor to execute SQL commands
    cursor1 = db1.cursor()
    cursor2 = db2.cursor()

    db1_data = cursor1.execute("SELECT * FROM crc32").fetchall()
    db2_data = cursor2.execute("SELECT * FROM crc32").fetchall()
else:
    print("\nThe scanned file database is missing or too small to contain useful information. Would "
          "you like to run scanner.py to run a scan right now? (y/Y or any key to "
          "exit): ")
    answer = input()
    if answer.lower() == 'y' or 'Y':
        system('python scanner.py')

    else:
        exit()

# Divide the data into multiple sets
num_cores = multiprocessing.cpu_count()
db1_data_sets = [db1_data[i:i + len(db1_data) // num_cores] for i in
                 range(0, len(db1_data), len(db1_data) // num_cores)]
db2_data_sets = [db2_data[i:i + len(db2_data) // num_cores] for i in
                 range(0, len(db2_data), len(db2_data) // num_cores)]

# Create a Queue to store the discrepancies
discrepancy_queue = multiprocessing.Queue()

# Create a Value to store the number of tested files
tested_files = multiprocessing.Value('i', 0)


# Create multiple processes to compare the data
def compare_data(db1_dat, db2_dat, discrepancy_q, fTested):
    fOK = 0
    discrepanc = []
    for row1 in db1_dat:
        with fTested.get_lock():
            fTested.value += 1
        for row2 in db2_dat:
            if row1[0] == row2[0] and row1[1] == row2[1]:  # Compare file names and hashes
                fOK += 1
            elif row1[0] == row2[0] and row1[1] != row2[1]:
                discrepanc = ["\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                              "!!!!!!!!!!!!!!" + "\nDiscrepancy in file hash of "
                              "'{}'".format(row1[0]), "Hash from Database 1: {}".format(row1[1]), "Hash from Database "
                              "2: {}\n".format(row2[1]) + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                                                          "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + "\n"]
    discrepancy_q.put(discrepanc)
    return discrepanc


if __name__ == '__main__':
    multiprocessing.freeze_support()

    # Create and start processes
    processes = []
    num_processes = min(multiprocessing.cpu_count(),
                        len(db1_data) // num_cores)  # Get the number of available CPU cores or entries in the databases
    for i in range(num_processes):
        p = multiprocessing.Process(target=compare_data,
                                    args=(db1_data_sets[i], db2_data_sets[i], discrepancy_queue, tested_files))
        processes.append(p)
        p.start()

    # Wait for the processes to finish
    for process in processes:
        process.join()

    # Retrieve the discrepancies from the queue
    discrepancies = []
    for i in range(num_processes):
        discrepancies += discrepancy_queue.get()

    # Print all the discrepancies
    for discrepancy in discrepancies:
        print(discrepancy)

    # Print the number of files tested
    ok_files = tested_files.value - len(discrepancies)
    print("Total files tested: {}\nFiles OK: {}\nFiles with discrepancies: {}\n".format(tested_files.value, ok_files,
                                                                                        len(discrepancies)))

    # Close the connection to the databases
    db1.close()
    db2.close()
