#!/usr/bin/env python3

from collections import OrderedDict
import sqlite3

from solarpi.utils import get_influxdb_params_from_env
from influxdb_client import Point, InfluxDBClient, WriteOptions
import rx
import rx.operators as ops

SQLITE_DB_PATH = "SolarPi.db"
BATCH_SIZE = 500
FLUSH_INTERVAL = 100

INFLUX = get_influxdb_params_from_env()


def parse_sdm_row(row: OrderedDict):
    return Point("sdm630") \
        .field("total_power_active", row[1]) \
        .time(row[0], write_precision='s')


def parse_inv_row(row: OrderedDict, name):
    return Point(name) \
        .field("status", row[1]) \
        .field("generatorspannung", float(row[2])) \
        .field("generatorstrom", float(row[3])) \
        .field("generatorleistung", float(row[4])) \
        .field("netzspannung", float(row[5])) \
        .field("einspeisestrom", float(row[6])) \
        .field("einspeiseleistung", float(row[7])) \
        .field("temperatur", float(row[8])) \
        .field("tagesertrag", float(row[9])) \
        .time(row[0], write_precision='s')


names = [("sdm630", "sdm630"), ("WR1", "WR Garage"), ("WR2", "WR Schipf")]

connection = sqlite3.connect(SQLITE_DB_PATH)
cursor = connection.cursor()

with InfluxDBClient(url=INFLUX.url, token=INFLUX.token, org=INFLUX.organisation, timeout=100000) as client:
    print("Connected to InfluxDB!")
    write_options = WriteOptions(batch_size=BATCH_SIZE, flush_interval=FLUSH_INTERVAL)
    with client.write_api(write_options=write_options) as write_api:
        print("Migrating...")
        for old_name, new_name in names:
            print(old_name+"...")
            values = cursor.execute('SELECT * FROM {}'.format(old_name)).fetchall()
            obs = rx.from_iterable(values).pipe(ops.map(lambda row: parse_sdm_row(row) if old_name == "sdm630" else parse_inv_row(row, new_name)))
            write_api.write(bucket=INFLUX.bucket, record=obs)
            obs.run()
print("Finished migration!")
