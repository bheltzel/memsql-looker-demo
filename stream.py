import mysql.connector
import time
from time import strftime, gmtime
import config

conn = mysql.connector.connect(
    user=config.config['memsql_user']
    , password=config.config['memsql_pw']
    , host=config.config['memsql_host']
    , database=config.config['memsql_db']
)

cursor = conn.cursor()
query = ("select count(1) as cnt from events;")

while True:    
    cursor.execute(query)
    for row in cursor:
        records = str("{:,}".format(row[0]))
        now = str(time.time())
        now = strftime("%d %b %Y %H:%M:%S", gmtime())
        print( now + ': Rows loaded to MemSQL ---> ' + records)
    time.sleep(1)

cursor.close()
conn.close()
