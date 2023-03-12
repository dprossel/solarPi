#!/usr/bin/env python3
import serial
import solarpi
from solarpi.utils import get_environment_variables, get_influxdb_params_from_env, get_mqtt_params_from_env
from sdm_modbus import SDM630
import sdm_modbus
import time
import sys

def main():
    env = get_environment_variables(["SERIAL_DEVICE", "SERIAL_BAUDRATE", "INTERVAL", "SDM_ADDR", "INV1_ADDR", "INV2_ADDR"])
    influx_params = get_influxdb_params_from_env()
    mqtt_params = get_mqtt_params_from_env()

    sdm = SDM630(device=env["SERIAL_DEVICE"], baud=int(env["SERIAL_BAUDRATE"]), parity="N", timeout=2, unit=int(env["SDM_ADDR"]))
    serial_port = serial.Serial(env["SERIAL_DEVICE"], int(env["SERIAL_BAUDRATE"]), parity="N", timeout=2)
    sdm.client.socket = serial_port

    energy_reader = solarpi.EnergyReader(sdm, sdm_modbus.registerType.INPUT, "sdm630")
    inverter1 = solarpi.KacoPowadorRs485(serial_port, int(env["INV1_ADDR"]), name="WR Garage")
    inverter2 = solarpi.KacoPowadorRs485(serial_port, int(env["INV2_ADDR"]), name="WR Schipf")

    serial_port2 = serial.Serial("/dev/serial/by-id/usb-1a86_USB2.0-Serial-if00-port0", 9600, parity="N", timeout=2)
    inverter3 = solarpi.SerialReadout(serial_port2, name="WR_Balkon")

    readers = [energy_reader, inverter1, inverter2, inverter3]
    
    clients = []
    clients.append(solarpi.InfluxDbWrapper(influx_params))
    clients.append(solarpi.MqttWrapper(mqtt_params))

    interval = int(env["INTERVAL"]) 
    while True:
        time_start = time.time()
        for reader in readers:
            meas = solarpi.Measurement(reader.name, reader.read_values())
            for client in clients:
                client.handle_measurement(meas)

        loop_time = time.time() - time_start
        time.sleep(max(interval - loop_time, 0))

if __name__ == "__main__":
    main()
