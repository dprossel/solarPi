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

    readers = [energy_reader, inverter1, inverter2]

    clients = []
    try:
        clients.append(solarpi.InfluxDbWrapper(influx_params))
    except:
        print("Could not connect to InfluxDb!")
    
    try:
        clients.append(solarpi.MqttWrapper(mqtt_params))
    except:
        print("Could not connect to MQTT!")

    if len(clients) == 0:
        print("Could not create any clients! Exiting")
        sys.exit(1)

    while True:
        time_start = time.process_time()
        for reader in readers:
            meas = solarpi.Measurement(reader.name(), reader.read_values())
            for client in clients:
                client.handle_measurement(meas)

        loop_time = time.process_time() - time_start
        print(loop_time)
        time.sleep(env["INTERVAL"] - loop_time)

if __name__ == "__main__":
    main()
