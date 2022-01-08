import abc
import datetime
import rx
from rx.core.typing import Observable
import rx.operators as ops
from influxdb_client import Point, InfluxDBClient, WriteOptions
import sdm_modbus
from dataclasses import dataclass
from abc import ABC
import serial
import threading



@dataclass
class InfluxDbParams:
    """Contains the necessary parameters to communicate with an InfluxDb instance.
    """
    url: str
    token: str
    organisation: str
    bucket: str


class Inverter(ABC):
    name: str

    @abc.abstractmethod
    def read_values(self):
        pass


class KacoPowadorRs485(Inverter):
    RESPONSE_LENGTH = 17
    GET_ALL_CMD = 9
    bus_address: int
    serialPort: serial.Serial

    def __init__(self, serial: serial.Serial, bus_address: int, name=None):
        self.bus_address = bus_address
        self.serialPort = serial
        if name is None:
            self.name = "Kaco Powador ({}:{})".format(serial.port, bus_address)

    def read_values(self, lock: threading.Lock = None):
        if lock is not None:
            with lock:
                result = self._do_read_values(1)
        else:
            result = self._do_read_values(1)

        if result is None:
            return {"status": -1,
                    "generatorspannung": -1.0,
                    "generatorstrom": -1.0,
                    "generatorleistung": -1.0,
                    "netzspannung": -1.0,
                    "einspeisestrom": -1.0,
                    "einspeiseleistung": -1.0,
                    "temperatur": -1.0,
                    "tagesertrag": -1.0}

        line = result.split("\r\n")[0]
        values = line.split()[1:10]
        return {"status": values[0],
                "generatorspannung": values[1],
                "generatorstrom": values[2]*1000,
                "generatorleistung": values[3],
                "netzspannung": values[4],
                "einspeisestrom": values[5]*1000,
                "einspeiseleistung": values[6],
                "temperatur": values[7],
                "tagesertrag": values[8]}

    def _do_read_values(self, retries):
        self.write_command(self.GET_ALL_CMD)
        result = self.serialPort.read(self.RESPONSE_LENGTH)
        if len(result) != self.RESPONSE_LENGTH:
            if retries > 0:
                return self._do_read_values(retries - 1)
            return None
        return result

    def write_command(self, command: int):
        return self.serialPort.write("#{:02d}{}\r".format(self.bus_address, command))


def read_sdm_energy_values(device: sdm_modbus.SDM630, lock: threading.Lock = None):
    """Read relevant energy values from SDM.
    """
    if lock is not None:
        with lock:
            results = device.read_all(sdm_modbus.registerType.INPUT)
    else:
        results = device.read_all(sdm_modbus.registerType.INPUT)
    print(results)
    return results


def convert_measurements_to_influxdb_point(name: str, measurements: dict):
    point = Point(name)
    point.time(datetime.datetime.now(datetime.timezone.utc))
    for key, val in measurements:
        point.field(key, val)
    return point


def get_sdm_energy_values_observable(
        device: sdm_modbus.SDM630, interval: float, lock: threading.Lock = None):
    return rx.interval(period=datetime.timedelta(seconds=interval)) \
        .pipe(ops.map(lambda _: read_sdm_energy_values(device, lock)),
              ops.map(lambda meas: convert_measurements_to_influxdb_point("sdm630", meas)))


def get_inverter_values_observable(device: Inverter, interval: float, lock: threading.Lock = None):
    return rx.interval(period=datetime.timedelta(seconds=interval)) \
        .pipe(ops.map(lambda _: device.read_values(lock)),
              ops.map(lambda meas: convert_measurements_to_influxdb_point(device.name, meas)))


def get_combined_observable(observables: list):
    return rx.merge(*observables)


def log_observable_to_influx_db(data: Observable, params: InfluxDbParams):
    with InfluxDBClient(url=params.url, token=params.token,
                        org=params.organisation) as db_client:
        with db_client.write_api(write_options=WriteOptions(batch_size=1)) as write_api:
            write_api.write(bucket=params.bucket, record=data)
            data.run()
