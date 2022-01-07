#!/usr/bin/env python3

from collections import OrderedDict
import sqlite3

from solarpi.utils import get_influxdb_params_from_env
from influxdb_client import Point, InfluxDBClient, WriteOptions
import rx
import rx.operators as ops

SQLITE_DB_PATH = "SolarPi.db"
BATCH_SIZE = 50000
FLUSH_INTERVAL = 10000

INFLUX = get_influxdb_params_from_env()


def parse_sdm_row(row: OrderedDict):
    return Point("sdm630") \
        .field("total_power_active", row[1]) \
        .time(row[0], write_precision='s')


def parse_inv_row(row: OrderedDict, name):
    return Point(name) \
        .field("status", row[1]) \
        .field("generatorspannung", row[2]) \
        .field("generatorstrom", row[3]) \
        .field("generatorleistung", row[4]) \
        .field("netzspannung", row[5]) \
        .field("einspeisestrom", row[6]) \
        .field("einspeiseleistung", row[7]) \
        .field("temperatur", row[8]) \
        .field("tagesertrag", row[9]) \
        .time(row[0], write_precision='s')


connection = sqlite3.connect(SQLITE_DB_PATH)
cursor = connection.cursor()
sdm_values = cursor.execute('SELECT * FROM sdm630').fetchall()
inv1_values = cursor.execute('SELECT * FROM WR1').fetchall()
inv2_values = cursor.execute('SELECT * FROM WR2').fetchall()

sdm_obs = rx.from_iterable(sdm_values).pipe(ops.map(lambda row: parse_sdm_row(row)))
inv1_obs = rx.from_iterable(inv1_values).pipe(ops.map(lambda row: parse_inv_row(row, "WR Garage")))
inv2_obs = rx.from_iterable(inv2_values).pipe(ops.map(lambda row: parse_inv_row(row, "WR Schipf")))


with InfluxDBClient(url=INFLUX.url, token=INFLUX.token, org=INFLUX.organisation) as client:
    print("Connected to InfluxDB!")
    write_options = WriteOptions(batch_size=BATCH_SIZE, flush_interval=FLUSH_INTERVAL)
    with client.write_api(write_options=write_options) as write_api:
        print("Migrating...")
        write_api.write(bucket=INFLUX.bucket, record=rx.merge(sdm_obs, inv1_obs, inv2_obs))
print("Finished migration!")
