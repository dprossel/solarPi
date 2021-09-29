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
from functools import wraps

import pdb


@dataclass
class InfluxDbParams:
    """Contains the necessary parameters to communicate with an InfluxDb instance.
    """
    url: str
    token: str
    organisation: str
    bucket: str


def _lock_func(func):
    @wraps(func)
    def locking_wrapper(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)
    return locking_wrapper


def _locking_io(cls):
    for key in dir(cls):
        if key in ["read", "write", "flush"]:
            value = getattr(cls, key)
            setattr(cls, key, _lock_func(value))
    return cls


@_locking_io
class ThreadSafeSerial(serial.Serial):
    def __init__(self, port=None, baudrate=9600, bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE, stopbits=serial.STOPBITS_ONE, timeout=None, xonxoff=False, rtscts=False, write_timeout=None, dsrdtr=False, inter_byte_timeout=None, exclusive=None, **kwargs):
        super().__init__(port=port, baudrate=baudrate, bytesize=bytesize, parity=parity, stopbits=stopbits, timeout=timeout, xonxoff=xonxoff,
                         rtscts=rtscts, write_timeout=write_timeout, dsrdtr=dsrdtr, inter_byte_timeout=inter_byte_timeout, exclusive=exclusive, **kwargs)
        self.lock = threading.Lock()


class Inverter(ABC):
    name: str

    @abc.abstractmethod
    def read_values():
        pass


class KacoPowadorRs485(object):
    RESPONSE_LENGTH = 17
    GET_ALL_CMD = 9
    bus_address: int
    serialPort: serial.Serial

    def __init__(self, serial: serial.Serial, bus_address: int, name=None):
        self.bus_address = bus_address
        self.serialPort = serial
        if name is None:
            self.name = "Kaco Powador ({}:{})".format(serial.port, bus_address)

    def read_values(self):
        self.write_command(self.GET_ALL_CMD)
        self.serialPort.read(self.RESPONSE_LENGTH)
        self.serialPort.close()
        return {"ertrag": 0}

    def write_command(self, command: int):
        return self.serialPort.write("#{:02d}{}\r".format(self.bus_address, command))


def read_sdm_energy_values(device: sdm_modbus.SDM630):
    """Read relevant energy values from SDM.
    """
    results = device.read_all(sdm_modbus.registerType.INPUT)
    print(results)
    return results


def convert_measurements_to_influxdb_point(name: str, measurements: dict):
    point = Point(name)
    point.time(datetime.datetime.now(datetime.timezone.utc))
    for key, val in measurements:
        point.field(key, val)
    return point


def get_sdm_energy_values_observable(device: sdm_modbus.SDM630, interval: float):
    return rx.interval(period=datetime.timedelta(seconds=interval)) \
        .pipe(ops.map(lambda _: read_sdm_energy_values(device)),
              ops.map(lambda meas: convert_measurements_to_influxdb_point("sdm630", meas)))


def get_inverter_values_observable(device: Inverter, interval: float):
    return rx.interval(period=datetime.timedelta(seconds=interval)) \
        .pipe(ops.map(lambda _: device.read_values()),
              ops.map(lambda meas: convert_measurements_to_influxdb_point(device.name, meas)))


def get_combined_observable(observables: list):
    return rx.merge(*observables)


def log_observable_to_influx_db(data: Observable, params: InfluxDbParams):
    with InfluxDBClient(url=params.url, token=params.token,
                        org=params.organisation, debug=True) as db_client:
        with db_client.write_api(write_options=WriteOptions(batch_size=1)) as write_api:
            write_api.write(bucket=params.bucket, record=data)
            data.run()
