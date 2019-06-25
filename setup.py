import mysql.connector
import time
from time import strftime, gmtime
import pprint as pp
import json
from lookerapi import LookerApi
import config

memsql_host = config.config['memsql_host']
memsql_user = config.config['memsql_user']
memsql_pw = config.config['memsql_pw']

looker_conn = config.config['looker_conn']
looker_host = config.config['looker_host']
looker_token = config.config['looker_token']
looker_secret = config.config['looker_secret']

# connect to memsql
conn = mysql.connector.connect(user=memsql_user, password=memsql_pw, host=memsql_host)
cursor = conn.cursor()

# create the memsql db and table
query = ("""
    DROP DATABASE IF EXISTS stocks; 
    CREATE DATABASE stocks; 
    USE stocks;

    CREATE TABLE ticks (
    tick VARCHAR(8) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
    dt DATETIME DEFAULT NULL,
    start FLOAT DEFAULT NULL,
    high FLOAT DEFAULT NULL,
    low FLOAT DEFAULT NULL,
    close FLOAT DEFAULT NULL,
    volume int(11) DEFAULT NULL,
    SHARD KEY tick (tick, dt)
    );
""")
cursor.execute(query)

# TODO: does not work yet
# create the memsql pipeline
query = ("USE stocks; CREATE PIPELINE ticks AS LOAD DATA KAFKA 'public-kafka.memcompute.com:9092/stockticker' BATCH_INTERVAL 2500 REPLACE INTO TABLE ticks FIELDS TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY '\\' LINES TERMINATED BY '\n' STARTING BY '';")
cursor.execute(query)

# TODO: does not work yet
# start the memsql pipeline
query = ("ALTER PIPELINE ticks SET OFFSETS LATEST; START PIPELINE ticks;")
cursor.execute(query)

cursor.close()
conn.close()

# create a looker session
looker = LookerApi(host=looker_host, token=looker_token, secret=looker_secret)
if looker is None:
    print('Connection to Looker failed')
    exit()

# TODO: does not work yet
# set the connection in looker to use the new memsql host
r = looker.update_connection(looker_conn, memsql_host, memsql_user, memsql_pw)
pp.pprint(r)