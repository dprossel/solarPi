#!/usr/bin/env python3
import serial
from solarpi import (get_sdm_energy_values_observable, log_observable_to_influx_db,
                     get_inverter_values_observable, get_combined_observable)
import solarpi
from solarpi.utils import get_environment_variables, get_influxdb_params_from_env
from sdm_modbus import SDM630
import sdm_modbus
import threading
import multiprocessing
from rx.scheduler import ThreadPoolScheduler

import time

def main():
    env = get_environment_variables(["SERIAL_DEVICE", "SERIAL_BAUDRATE", "SDM_INTERVAL", "SDM_ADDR", "INV1_ADDR", "INV2_ADDR", "INV1_INTERVAL", "INV2_INTERVAL"])
    influx_params = get_influxdb_params_from_env()

    sdm = SDM630(device=env["SERIAL_DEVICE"], baud=int(env["SERIAL_BAUDRATE"]), parity="N", timeout=2, unit=int(env["SDM_ADDR"]))
    serial_port = serial.Serial(env["SERIAL_DEVICE"], int(env["SERIAL_BAUDRATE"]), parity="N", timeout=2)
    sdm.client.socket = serial_port
    values = [key for key in sdm.read_all(sdm_modbus.registerType.INPUT).keys()]

    inverter1 = solarpi.KacoPowadorRs485(serial_port, int(env["INV1_ADDR"]), name="WR Garage")
    inverter2 = solarpi.KacoPowadorRs485(serial_port, int(env["INV2_ADDR"]), name="WR Schipf")

    scheduler = ThreadPoolScheduler(multiprocessing.cpu_count())
    serial_lock = threading.Lock()

    sdm_obs = get_sdm_energy_values_observable(sdm, float(env["SDM_INTERVAL"]), values, serial_lock, scheduler)
    inv1_obs = get_inverter_values_observable(inverter1, float(env["INV1_INTERVAL"]), serial_lock, scheduler)
    inv2_obs = get_inverter_values_observable(inverter2, float(env["INV2_INTERVAL"]), serial_lock, scheduler)

    log_observable_to_influx_db(get_combined_observable(
       [sdm_obs, inv1_obs, inv2_obs]), influx_params)
    #log_observable_to_influx_db(sdm_obs, influx_params)

if __name__ == "__main__":
    main()
