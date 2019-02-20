from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import uuid
import os
import gzip
import re
import datetime
import sys

log_dissemble = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
# Function to insert each line into cassandra as batches of a few hundred
def insert_newlog(log_file, table_name):
    count = 1
    insert_query = session.prepare("INSERT INTO " + table_name + " (host, id, datetime, path, bytes) VALUES (?, uuid(), ?, ?, ?)")
    batch = BatchStatement()
    for line in log_file:
        values = log_dissemble.split(line)
        if len(values) >= 4:  # Only consider lines which can be split as host, dtime, path, num_bytes
            host = values[1]
            dtime = datetime.datetime.strptime(values[2], '%d/%b/%Y:%H:%M:%S')
            path = values[3]
            num_bytes = int(values[4])
            count += 1
            batch.add(insert_query, (host, dtime, path, num_bytes))
            if count == 300:
                session.execute(batch)
                batch.clear()
                count = 1
    session.execute(batch)
    batch.clear()

def main(inputs,table_name):
    for f in os.listdir(inputs):
        filename, file_extension = os.path.splitext(f)
        file = os.path.join(inputs, f)
        if file_extension == '.gz':  # If the file type is in .gz format
            with gzip.open(file, 'rt', encoding='utf-8', errors='ignore') as logfile:
                insert_newlog(logfile, table_name)
        else:
            insert_newlog(open(file), table_name)   # If file is not in .gz format


if __name__ == '__main__':
    inputs = sys.argv[1]
    user_id = sys.argv[2]
    table_name = sys.argv[3]
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])  # Connect to the cluster
    session = cluster.connect(user_id)  # Use the specified key space
    main(inputs,table_name)

    
