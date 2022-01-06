#!/usr/bin/env python3

from collections import OrderedDict
import sqlite3
from solarpi.utils import get_influxdb_params_from_env
from influxdb_client import Point, InfluxDBClient, WriteOptions
import rx
import rx.operators as ops

SQLITE_DB_PATH = "SolarPi.db"
BATCH_SIZE = 50_000
FLUSH_INTERVAL = 10_000

INFLUX = get_influxdb_params_from_env()


def parse_sdm_row(row: OrderedDict):
    return Point("sdm630") \
        .field("total_power_active", float(row['verbrauch'])) \
        .time(row['timestamp'])


def parse_inv_row(row: OrderedDict, name):
    return Point(name) \
        .field("status", float(row['status'])) \
        .field("generatorspannung", float(row['generatorspannung'])) \
        .field("generatorstrom", float(row['generatorstrom'])) \
        .field("generatorleistung", float(row['generatorleistung'])) \
        .field("netzspannung", float(row['netzspannung'])) \
        .field("einspeisestrom", float(row['einspeisestrom'])) \
        .field("einspeiseleistung", float(row['einspeiseleistung'])) \
        .field("temperatur", float(row['temperatur'])) \
        .field("tagesertrag", float(row['tagesertrag'])) \
        .time(row['timestamp'])


connection = sqlite3.connect(SQLITE_DB_PATH)
cursor = connection.cursor()
sdm_values = cursor.execute('SELECT * FROM sdm630')
inv1_values = cursor.execute('SELECT * FROM WR1')
inv2_values = cursor.execute('SELECT * FROM WR2')

sdm_obs = rx.from_iterable(sdm_values).pipe(ops.map(lambda row: parse_sdm_row(row)))
inv1_obs = rx.from_iterable(inv1_values).pipe(ops.map(lambda row: parse_inv_row(row, "WR Garage")))
inv2_obs = rx.from_iterable(inv2_values).pipe(ops.map(lambda row: parse_inv_row(row, "WR Schipf")))

with InfluxDBClient(url=INFLUX.url, token=INFLUX.token, org=INFLUX.organisation, debug=True) as client:
    write_options = WriteOptions(batch_size=BATCH_SIZE, flush_interval=FLUSH_INTERVAL)
    with client.write_api(write_options=write_options) as write_api:
        write_api.write(bucket=INFLUX.bucket, record=rx.merge(sdm_obs, inv1_obs, inv2_obs))
