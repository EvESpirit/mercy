import sqlite3
import multiprocessing as mp
from os import path, stat, system
from sys import exit

# Check if reference.db and scanned.db exist
if not path.isfile('reference.db') \
        or not path.isfile('scanned.db') \
        or stat('reference.db').st_size < 16384 \
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

    db1D = cursor1.execute("SELECT * FROM crc32").fetchall()
    db2D = cursor2.execute("SELECT * FROM crc32").fetchall()
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
cores = mp.cpu_count()
db1_data_sets = [db1D[i:i + len(db1D) // cores] for i in
                 range(0, len(db1D), len(db1D) // cores)]
db2_data_sets = [db2D[i:i + len(db2D) // cores] for i in
                 range(0, len(db2D), len(db2D) // cores)]

# Create a Queue to store the discrepancies
resultQu = mp.Queue()

# Create a Value to store the number of tested files
tested_files = mp.Value('i', 0)

# Create a global variable to store user input
choice = None

# Ask the user for input
def uC():
    global choice
    choice = input("Do you wish to scan log extensions as well? These are changed often by the system internally and hence "
              "are very likely to show up during scans. (y/n): ")

    if choice == "y" or choice == "Y":
        print("\nLog extensions will be scanned.")
        return True
    else:
        print("\nLog extensions will not be scanned.")
        return False

# Create multiple processes to compare the data
def dCompareSmart(dbaData, dbdData, resultQ, fTested):
    fOK = 0
    result = []
    logExtensions = set(['.evtx', '.log', '.txt', '.db'])  # Create a set of extensions

    for row1 in dbaData:
        # Check for file extensions and skip any that match
        if not row1[0].endswith(tuple(logExtensions)):
            with fTested.get_lock():
                fTested.value += 1
            for row2 in dbdData:
                if row1[0] == row2[0] and row1[1] == row2[1]:  # Compare file names and hashes
                    fOK += 1
                elif row1[0] == row2[0] and row1[1] != row2[1]:
                    result = [
                        "\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                        "!!!!!!!!!!!!!!" + "\nDiscrepancy in file hash of "
                                           "'{}'".format(row1[0]), "Hash from Database 1: {}".format(row1[1]),
                        "Hash from Database "
                        "2: {}\n".format(row2[1]) + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                                                    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + "\n"]
    resultQ.put(result)
    return result


def dCompareAll(dbaData, dbdData, resultQ, fTested):
    fOK = 0
    result = []

    for row1 in dbaData:
        with fTested.get_lock():
            fTested.value += 1
        for row2 in dbdData:
            if row1[0] == row2[0] and row1[1] == row2[1]:  # Compare file names and hashes
                fOK += 1
            elif row1[0] == row2[0] and row1[1] != row2[1]:
                result = [
                    "\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                    "!!!!!!!!!!!!!!" + "\nDiscrepancy in file hash of "
                                       "'{}'".format(row1[0]), "Hash from Database 1: {}".format(row1[1]),
                    "Hash from Database "
                    "2: {}\n".format(row2[1]) + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                                                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + "\n"]
    resultQ.put(result)
    return result


if __name__ == '__main__':
    choice = uC()
    mp.freeze_support()

    # Create and start processes
    processes = []
    # Get the number of available CPU cores or entries in the databases
    num_processes = min(mp.cpu_count(), len(db1D) // cores)

    for i in range(num_processes):
        if choice:
            p = mp.Process(target=dCompareAll, args=(db1_data_sets[i], db2_data_sets[i], resultQu, tested_files))
        else:
            p = mp.Process(target=dCompareSmart, args=(db1_data_sets[i], db2_data_sets[i], resultQu, tested_files))
        processes.append(p)
        p.start()

    # Wait for the processes to finish
    for process in processes:
        process.join()

    # Retrieve the discrepancies from the queue
    discrepancies = []
    for i in range(num_processes):
        discrepancies += resultQu.get()

    # Print all the discrepancies
    for discrepancy in discrepancies:
        print(discrepancy)

    # Print the number of files tested
    ok_files = tested_files.value - len(discrepancies)
    print("\nTotal files tested: {}\nFiles OK: {}\nFiles with discrepancies: {}\n".format(tested_files.value, ok_files,
                                                                                          len(discrepancies)))

    # Close the connection to the databases
    db1.close()
    db2.close()
