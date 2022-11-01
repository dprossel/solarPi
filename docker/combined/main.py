#!/usr/bin/env python3
import serial
from solarpi import (get_measurement_observable,
                     get_combined_observable)
import solarpi
from solarpi.utils import get_environment_variables, get_influxdb_params_from_env, get_mqtt_params_from_env
from sdm_modbus import SDM630
import sdm_modbus
import threading
import multiprocessing
from reactivex.scheduler import ThreadPoolScheduler

def main():
    env = get_environment_variables(["SERIAL_DEVICE", "SERIAL_BAUDRATE", "SDM_INTERVAL", "SDM_ADDR", "INV1_ADDR", "INV2_ADDR", "INV1_INTERVAL", "INV2_INTERVAL"])
    influx_params = get_influxdb_params_from_env()
    mqtt_params = get_mqtt_params_from_env()

    sdm = SDM630(device=env["SERIAL_DEVICE"], baud=int(env["SERIAL_BAUDRATE"]), parity="N", timeout=2, unit=int(env["SDM_ADDR"]))
    serial_port = serial.Serial(env["SERIAL_DEVICE"], int(env["SERIAL_BAUDRATE"]), parity="N", timeout=2)
    sdm.client.socket = serial_port

    energy_reader = solarpi.EnergyReader(sdm, sdm_modbus.registerType.INPUT, "sdm630")
    inverter1 = solarpi.KacoPowadorRs485(serial_port, int(env["INV1_ADDR"]), name="WR Garage")
    inverter2 = solarpi.KacoPowadorRs485(serial_port, int(env["INV2_ADDR"]), name="WR Schipf")

    scheduler = ThreadPoolScheduler(multiprocessing.cpu_count())
    serial_lock = threading.Lock()

    sdm_obs = get_measurement_observable(energy_reader, float(env["SDM_INTERVAL"]), 1, serial_lock, scheduler)
    inv1_obs = get_measurement_observable(inverter1, float(env["INV1_INTERVAL"]), 1, serial_lock, scheduler)
    inv2_obs = get_measurement_observable(inverter2, float(env["INV2_INTERVAL"]), 1, serial_lock, scheduler)
    combined_data = get_combined_observable([sdm_obs, inv1_obs, inv2_obs])
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
        return

    for client in clients:
        client.subscribe(combined_data)
    #combined_data.subscribe(lambda x: print(x), on_error=lambda x: print(x))
    combined_data.connect()
    combined_data.run()


if __name__ == "__main__":
    main()
