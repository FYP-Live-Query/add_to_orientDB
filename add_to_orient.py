import csv
import time
import random
from pyorient import OrientDB

# Connect to the OrientDB server
client = OrientDB("10.8.100.246", 2424)
client.set_session_token(True)
session_id = client.connect('root', 'live')
# Open the database
client.db_open("networkTraffic", "root", "live")

# Specify the path to the CSV file
csv_file_path = "records.csv"

# Open the CSV file
with open(csv_file_path, "r") as file:
    browsers = ['chr', 'fox', 'mie', 'opr', 'saf']
    # Create a CSV reader
    reader = csv.reader(file)
    next(reader)
    # Iterate over each row in the CSV file
    for row in reader:
        client.command("INSERT INTO Network(ip, date , timestamp , browser ,traffic , eventTimestamp) "
      "VALUES('%s', '%s', '%s','%s', '%s', sysDate())"
      % (row[0],row[1],row[2],browsers[random.randint(0,4)],float(row[-7])))
        print(row)
        # Add a one-second delay before the next iteration
        time.sleep(1)

# Close the database connection
client.db_close()

# Disconnect from the server
client.disconnect()

