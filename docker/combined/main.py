#!/usr/bin/env python3
import serial
from solarpi import (get_sdm_energy_values_observable, log_observable_to_influx_db,
                     InfluxDbParams, get_inverter_values_observable, get_combined_observable)
import solarpi
from solarpi.utils import get_environment_variables, get_influxdb_params_from_env
from sdm_modbus import SDM630
import threading


def main():
    env = get_environment_variables(["SERIAL_DEVICE", "SERIAL_BAUDRATE", "SDM_INTERVAL"])
    influx_params = get_influxdb_params_from_env()

    serial_port = solarpi.ThreadSafeSerial(port=env["SERIAL_DEVICE"], baudrate=int(env["SERIAL_BAUDRATE"]))

    sdm = SDM630(device=env["SERIAL_DEVICE"], baud=int(env["SERIAL_BAUDRATE"]), parity="N")
    sdm.client.socket = serial_port

    inverter1 = solarpi.KacoPowadorRs485(serial_port, env["INV1_ADDR"])
    inverter2 = solarpi.KacoPowadorRs485(serial_port, env["INV2_ADDR"])

    serial_port.open()

    serial_lock = threading.Lock()
    sdm_obs = get_sdm_energy_values_observable(sdm, float(env["SDM_INTERVAL"]), serial_lock)
    inv1_obs = get_inverter_values_observable(inverter1, float(env["INV1_INTERVAL"]), serial_lock)
    inv2_obs = get_inverter_values_observable(inverter2, float(env["INV2_INTERVAL"]), serial_lock)

    log_observable_to_influx_db(get_combined_observable(
        [sdm_obs, inv1_obs, inv2_obs]), influx_params)


if __name__ == "__main__":
    main()
