import os
import sys
import datetime
import rx
from rx.core.typing import Observable
import rx.operators as ops
from influxdb_client import Point, InfluxDBClient, WriteOptions


def get_environment_variables(variables: list):
    try:
        return {var: os.environ[var] for var in variables}
    except KeyError as err:
        print("Environment variable(s) not set!")
        print("Expected these variables: " + str(variables))
        print("But found only these: " + str(list(os.environ.keys())))
        print("Exiting...")
        sys.exit(1)


def read_sdm_energy_values():
    """Read relevant energy values from SDM.
    """
    return [1, 2, 3]


def convert_sdm_values_to_influxdb_point(measurements):
    return Point("financial-analysis") \
        .tag("type", "vix-daily") \
        .field("open", measurements[0]) \
        .field("high", measurements[1]) \
        .field("low", measurements[2]) \
        .time(datetime.datetime.now(datetime.timezone.utc))


def get_sdm_energy_values_observable(interval: float):
    return rx.interval(period=datetime.timedelta(seconds=interval)) \
        .pipe(ops.map(lambda _: read_sdm_energy_values()),
              ops.map(lambda measurement: convert_sdm_values_to_influxdb_point(measurement)))


def log_observable_to_influx_db(data: Observable):
    env = get_environment_variables(
        ["INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG", "INFLUXDB_BUCKET"])

    with InfluxDBClient(url=env["INFLUXDB_URL"], token=env["INFLUXDB_TOKEN"],
                        org=env["INFLUXDB_ORG"], debug=True) as db_client:
        with db_client.write_api(write_options=WriteOptions(batch_size=1)) as write_api:
            write_api.write(bucket=env["INFLUXDB_BUCKET"], record=data)
            data.run()
