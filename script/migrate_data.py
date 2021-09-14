#!/usr/bin/env python3

from collections import OrderedDict
import sqlite3

import rx
from rx import operators as ops

from influxdb_client import Point, InfluxDBClient, WriteOptions


SQLITE_DB_PATH = "SolarPi.db"
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = ""
INFLUXDB_ORG = "prossel"
INFLUXDB_BUCKET = "solarpi"

BATCH_SIZE = 50_000
FLUSH_INTERVAL = 10_000


def parse_row(row: OrderedDict):
    return Point("financial-analysis") \
        .tag("type", "vix-daily") \
        .field("open", float(row['VIX Open'])) \
        .field("high", float(row['VIX High'])) \
        .field("low", float(row['VIX Low'])) \
        .field("close", float(row['VIX Close'])) \
        .time(row['Date'])


connection = sqlite3.connect(SQLITE_DB_PATH)
cursor = connection.cursor()
results = cursor.execute('SELECT * FROM ')

data = rx \
    .from_iterable(results) \
    .pipe(ops.map(lambda row: parse_row(row)))

with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG,
                    debug=True) as client:

    write_options = WriteOptions(batch_size=BATCH_SIZE, flush_interval=FLUSH_INTERVAL)
    with client.write_api(write_options=write_options) as write_api:
        write_api.write(bucket=INFLUXDB_BUCKET, record=data)
