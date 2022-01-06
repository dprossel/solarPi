#!/usr/bin/env python3
import serial
from solarpi import (get_sdm_energy_values_observable, log_observable_to_influx_db,
                     InfluxDbParams, get_inverter_values_observable, get_combined_observable)
import solarpi
from solarpi.utils import get_environment_variables, get_influxdb_params_from_env
from sdm_modbus import SDM630


def main():
    env = get_environment_variables(["SERIAL_DEVICE", "SERIAL_BAUDRATE", "SDM_INTERVAL"])
    influx_params = get_influxdb_params_from_env()

    serial_port = solarpi.ThreadSafeSerial(port=env["SERIAL_DEVICE"], baudrate=env["SERIAL_BAUDRATE"])

    sdm = SDM630(device=env["SERIAL_DEVICE"], baud=env["SERIAL_BAUDRATE"])
    sdm.client.socket = serial_port

    inverter1 = solarpi.KacoPowadorRs485(serial_port, env["INV1_ADDR"])
    inverter2 = solarpi.KacoPowadorRs485(serial_port, env["INV2_ADDR"])

    serial_port.open()

    sdm_obs = get_sdm_energy_values_observable(sdm, env["SDM_INTERVAL"])
    inv1_obs = get_inverter_values_observable(inverter1, env["INV1_INTERVAL"])
    inv2_obs = get_inverter_values_observable(inverter2, env["INV2_INTERVAL"])

    log_observable_to_influx_db(get_combined_observable(
        [sdm_obs, inv1_obs, inv2_obs]), influx_params)


if __name__ == "__main__":
    main()
