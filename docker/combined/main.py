#!/usr/bin/env python3

import atexit
from datetime import timedelta
from threading import Lock

import rx
from rx import operators as ops

from influxdb_client import Point, InfluxDBClient, WriteApi, WriteOptions


INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = ""
INFLUXDB_ORG = "prossel"
INFLUXDB_BUCKET = "solarpi"


def on_exit(db_client: InfluxDBClient, write_api: WriteApi):
    """Close clients after terminate a script.
    :param db_client: InfluxDB client
    :param write_api: WriteApi
    :return: nothing
    """
    write_api.close()
    db_client.close()


def read_sdm_energy_values():
    """Read relevant energy values from SDM.
    """
    with socket_lock:
        return [1, 2, 3]


def read_inverter_values():
    with socket_lock:
        pass


def line_protocol(measurements):
    """Create a InfluxDB line protocol with structure:
        iot_sensor,hostname=mine_sensor_12,type=temperature value=68
    :param temperature: the sensor temperature
    :return: Line protocol to write into InfluxDB
    """

    return Point("financial-analysis") \
        .tag("type", "vix-daily") \
        .field("open", float(measurements['VIX Open'])) \
        .field("high", float(measurements['VIX High'])) \
        .field("low", float(measurements['VIX Low'])) \
        .field("close", float(measurements['VIX Close'])) \
        .time(measurements['Date'])


socket_lock = Lock()

energy_data = rx \
    .interval(period=timedelta(seconds=60)) \
    .pipe(ops.map(lambda t: read_sdm_energy_values()),
          ops.distinct_until_changed(),
          ops.map(lambda temperature: line_protocol(temperature)))

inverter_data = rx \
    .interval(period=timedelta(seconds=60)) \
    .pipe(ops.map(lambda t: read_inverter_values()),
          ops.distinct_until_changed(),
          ops.map(lambda temperature: line_protocol(temperature)))

data = rx.merge(energy_data, inverter_data)

_db_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG, debug=True)

_write_api = _db_client.write_api(write_options=WriteOptions(batch_size=1))
_write_api.write(bucket=INFLUXDB_BUCKET, record=data)

atexit.register(on_exit, _db_client, _write_api)
