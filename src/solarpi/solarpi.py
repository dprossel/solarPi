import abc
import datetime
import rx
from rx.core.typing import Observable
import rx.operators as ops
from influxdb_client import Point, InfluxDBClient, WriteOptions
from paho.mqtt import client as mqtt_client
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


@dataclass
class MqttParams:
    """Contains the necessary parameters to publish data via MQTT.
    """
    broker: str
    port: int
    client_id: str

    def __post_init__(self):
        self.port = int(self.port)


class Inverter(ABC):
    name: str

    @abc.abstractmethod
    def read_values(self):
        pass


class KacoPowadorRs485(Inverter):
    RESPONSE_LENGTH = 66
    GET_ALL_CMD = 0
    bus_address: int
    serialPort: serial.Serial

    def __init__(self, serial: serial.Serial, bus_address: int, name=None):
        self.bus_address = bus_address
        self.serialPort = serial

        self.name=name
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

        values = result.split()[1:10]
        return {"status": int(values[0]),
                "generatorspannung": float(values[1]),
                "generatorstrom": float(values[2])*1000,
                "generatorleistung": float(values[3]),
                "netzspannung": float(values[4]),
                "einspeisestrom": float(values[5])*1000,
                "einspeiseleistung": float(values[6]),
                "temperatur": float(values[7]),
                "tagesertrag": float(values[8])}

    def _do_read_values(self, retries):
        if not self.serialPort.is_open:
            self.serialPort.open()
        self.write_command(self.GET_ALL_CMD)
        result = self.serialPort.read(self.RESPONSE_LENGTH)
        if len(result) != self.RESPONSE_LENGTH:
            print("Wrong response length", len(result))
            if retries > 0:
                return self._do_read_values(retries - 1)
            return None
        return result

    def write_command(self, command: int):
        return self.serialPort.write(str.encode("#{:02d}{}\r".format(self.bus_address, command)))


def read_sdm_energy_values(device: sdm_modbus.SDM630, values: list, lock: threading.Lock = None):
    """Read relevant energy values from SDM.
    """
    if lock is not None:
        with lock:
            results = {register: device.read(register) for register in values}
            #results = device.read_all(sdm_modbus.registerType.INPUT)
    else:
        results = {register: device.read(register) for register in values}
        #results = device.read_all(sdm_modbus.registerType.INPUT)
    return results


def convert_measurements_to_influxdb_point(name: str, measurements: dict):
    point = Point(name)
    point.time(datetime.datetime.now(datetime.timezone.utc))
    for key, val in measurements.items():
        point.field(key, val)
    return point


def get_sdm_energy_values_observable(
        device: sdm_modbus.SDM630, interval: float, values: list, lock: threading.Lock = None, scheduler = None):
    return rx.interval(period=datetime.timedelta(seconds=interval), scheduler=scheduler) \
        .pipe(ops.map(lambda _: read_sdm_energy_values(device, values, lock)),
              ops.map(lambda meas: convert_measurements_to_influxdb_point("sdm630", meas)))


def get_inverter_values_observable(device: Inverter, interval: float, lock: threading.Lock = None, scheduler = None):
    return rx.interval(period=datetime.timedelta(seconds=interval), scheduler=scheduler) \
        .pipe(ops.map(lambda _: device.read_values(lock)),
              ops.map(lambda meas: convert_measurements_to_influxdb_point(device.name, meas)))


def get_combined_observable(observables: list):
    return rx.merge(*observables)


def subscribe_influx_db_to_observable(data: Observable, params: InfluxDbParams):
    client = InfluxDBClient(url=params.url, token=params.token, org=params.organisation)
    write_api = client.write_api(write_options=WriteOptions(batch_size=1))
    write_api.write(bucket=params.bucket, record=data)
    return write_api


def influxdb_point_to_mqtt(point):
    topics = [point._name + "/" + k for k in point._fields.keys()]
    return [topics, point._fields.values()]


def subscribe_mqtt_to_observable(data: Observable, params: MqttParams):
    def on_connect(client, userdata, flags, rc):
        if rc != 0:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(params.client_id)
    client.on_connect = on_connect
    client.connect(params.broker, params.port)
    data.subscribe(on_next = lambda point: list(map(client.publish, *influxdb_point_to_mqtt(point))),
                   on_error = lambda e: print("Error : {0}".format(e)))
    return client

