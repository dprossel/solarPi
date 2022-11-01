import abc
import datetime
import reactivex as rx
import reactivex.operators as ops
from reactivex.abc.scheduler import SchedulerBase
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


@dataclass
class Measurement:
    """The item type in the observable measurement stream obtained from an inverter or energy meter.
    """
    device_name: str
    values: dict


class SerialReader(ABC):
    def __init__(self, name):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def read_values(self, retries: int = 1, lock: threading.Lock = None) -> dict:
        if lock is None:
            result = self._do_read_values()
        else:
            with lock:
                result = self._do_read_values()
        if result is None and retries > 0:
            return self.read_values(retries - 1, lock)
        return result

    @abc.abstractmethod
    def _do_read_values(self) -> dict:
        pass


class KacoPowadorRs485(SerialReader):
    RESPONSE_LENGTH = 66
    GET_ALL_CMD = 0

    def __init__(self, serial: serial.Serial, bus_address: int, name: str = None):
        super().__init__(name)
        self._bus_address = bus_address
        self._serialPort = serial

        if self._name is None:
            self._name = "Kaco Powador ({}:{})".format(
                serial.port, bus_address)

    def _do_read_values(self) -> dict:
        if not self._serialPort.is_open:
            self._serialPort.open()
        self.write_command(self.GET_ALL_CMD)
        result = self._serialPort.read(self.RESPONSE_LENGTH)
        if len(result) != self.RESPONSE_LENGTH:
            print("Wrong response length", len(result))
            return None

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

    def write_command(self, command: int) -> int:
        return self._serialPort.write(str.encode("#{:02d}{}\r".format(self._bus_address, command)))


class EnergyReader(SerialReader):
    """Read relevant energy values from SDM.
    """

    def __init__(self, device: sdm_modbus.SDM630, values: list, name: str = None):
        super().__init__(name)
        self._device = device

        if isinstance(values, sdm_modbus.registerType):
            values = [key for key in device.read_all(values).keys()]
        self._values = values

        if self._name is None:
            self._name = "{} ({}:{})".format(
                self._device.model, self._device.host, self._device.port)

    def _do_read_values(self) -> dict:
        return {register: self._device.read(register) for register in self._values}


class Wrapper(ABC):
    @abc.abstractmethod
    def subscribe(self, data: rx.Observable):
        pass


class MqttWrapper(Wrapper):
    """Wraps the mqtt client.
    """

    def __init__(self, params: MqttParams):
        self._data_handle = None
        self._client = mqtt_client.Client(params.client_id)
        self._client.on_connect = self._on_connect
        self._client.connect(params.broker, params.port)

    def subscribe(self, data: rx.Observable):
        self.data_handle = data.subscribe(
            on_next=self.handle_measurement, on_error=lambda e: print("Error : {0}".format(e)))
    def handle_measurement(self, measurement: Measurement):
        for key, value in measurement.values.items():
            topic = "{}/{}".format(measurement.device_name, key)
            self._client.publish(topic, value)

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            print("Failed to connect, return code %d\n", rc)


class InfluxDbWrapper(Wrapper):
    """Wraps the InfluxDb client.
    """

    def __init__(self, params: InfluxDbParams):
        client = InfluxDBClient(
            url=params.url, token=params.token, org=params.organisation)
        self._write_api = client.write_api(
            write_options=WriteOptions(batch_size=1))
        self._bucket = params.bucket

    def subscribe(self, data: rx.Observable):
        point_data = data.pipe(ops.map(self.handle_measurement))
        self._write_api.write(bucket=self._bucket, record=point_data)

    def handle_measurement(self, measurement: Measurement):
        point = Point(measurement.device_name)
        point.time(datetime.datetime.now(datetime.timezone.utc))
        for key, val in measurement.values.items():
            point.field(key, val)
        return point


def get_measurement_observable(
        reader: SerialReader, interval: float, retries: int = 1, lock: threading.Lock = None, scheduler: SchedulerBase = None) -> rx.Observable:
    return rx.interval(period=datetime.timedelta(seconds=interval), scheduler=scheduler) \
        .pipe(ops.map(lambda _: reader.read_values(retries, lock)), ops.filter(lambda v: v is not None), ops.map(lambda v: Measurement(reader.name, v)))


def get_combined_observable(observables: list) -> rx.Observable:
    return rx.merge(*observables).pipe(ops.publish())
