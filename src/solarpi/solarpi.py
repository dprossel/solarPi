import abc
import datetime
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

    def read_values(self, retries: int = 0, lock: threading.Lock = None) -> dict:
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
            #print("Wrong response length", len(result))
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


class SerialReadout(SerialReader):
    def __init__(self, serial: serial.Serial, name: str = None):
        super().__init__(name)
        self._serialPort = serial

        if self._name is None:
            self._name = "SerialReadout ({})".format(serial.port)

    def _do_read_values(self) -> dict:
        if not self._serialPort.is_open:
            self._serialPort.open()
        result = self._serialPort.read(self.RESPONSE_LENGTH)
        if len(result) != self.RESPONSE_LENGTH:
            #print("Wrong response length", len(result))
            return None

        values = result.split()[1:10]
        return {"leistung": int(values[0])}


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
    def __init__(self):
        self._do_init = True
        self._MAX_RETRIES = 4
        self._num_retries = self._MAX_RETRIES

    def handle_measurement(self, measurement: Measurement):
        if measurement.values is None:
            return

        if self._do_init:
            try:
                self.init()
            except Exception as e:
                self._num_retries -= 1
                if self._num_retries > 0:
                    self._do_init = True
                    print("Initialization failed. Remaining retries:", self._num_retries)
                else:
                    print("Giving up on trying to reconnect.")
                    self._do_init = False
            else:
                self._num_retries = self._MAX_RETRIES
                self._do_init = False
        
        if not self._do_init and self._num_retries > 0:
            try:
                self._do_handle_measurement(measurement)
            except Exception as e:
                self._do_init = True
                print("Client failed to publish:")
                print(e)

    @abc.abstractmethod
    def _do_handle_measurement(self, measurement: Measurement):
        pass

    @abc.abstractmethod
    def init(self):
        pass


class MqttWrapper(Wrapper):
    """Wraps the mqtt client.
    """

    def __init__(self, params: MqttParams):
        super().__init__()
        self._client = None
        self._params = params
    
    def _do_handle_measurement(self, measurement: Measurement):
        for key, value in measurement.values.items():
            topic = "{}/{}".format(measurement.device_name.lower().replace(" ", "_"), key)
            result = self._client.publish(topic, value)
            result.is_published()
    
    def init(self):
        self._client = mqtt_client.Client(self._params.client_id)
        self._client.on_connect = self._on_connect
        self._client.connect(self._params.broker, self._params.port)

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            print("Failed to connect, return code %d\n", rc)


class InfluxDbWrapper(Wrapper):
    """Wraps the InfluxDb client.
    """

    def __init__(self, params: InfluxDbParams):
        super().__init__()
        self._write_api = None
        self._params = params

    def _do_handle_measurement(self, measurement: Measurement):
        point = Point(measurement.device_name)
        point.time(datetime.datetime.now(datetime.timezone.utc))
        for key, val in measurement.values.items():
            point.field(key, val)
        self._write_api.write(self._params.bucket, record=point)
    
    def init(self):
        client = InfluxDBClient(
            url=self._params.url, token=self._params.token, org=self._params.organisation)
        self._write_api = client.write_api(
            write_options=WriteOptions(batch_size=1))

