#!/usr/bin/env python3
import serial
from solarpi import (get_sdm_energy_values_observable, log_observable_to_influx_db,
                     InfluxDbParams, get_inverter_values_observable, get_combined_observable)
import solarpi
from solarpi.utils import get_environment_variables
from sdm_modbus import SDM630


def main():
    env = get_environment_variables(
        ["INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG", "INFLUXDB_BUCKET", "SERIAL_DEVICE", "SERIAL_BAUDRATE", "SDM_INTERVAL"])

    influx_params = InfluxDbParams(url=env["INFLUXDB_URL"], token=env["INFLUXDB_TOKEN"],
                                   organisation=env["INFLUXDB_ORG"], bucket=env["INFLUXDB_BUCKET"])

    serial_port = solarpi.ThreadSafeSerial(port=env["SDM_DEVICE"], baudrate=env["SDM_BAUDRATE"])

    sdm = SDM630(device=env["SERIAL_DEVICE"], baud=env["SERIAL_BAUDRATE"])
    sdm.client.socket = serial_port

    inverter1 = solarpi.KacoPowadorRs485(serial_port, env["INV1_PORT"])
    inverter2 = solarpi.KacoPowadorRs485(serial_port, env["INV2_PORT"])

    serial_port.open()

    sdm_obs = get_sdm_energy_values_observable(sdm, env["SDM_INTERVAL"])
    inv1_obs = get_inverter_values_observable(inverter1, env["INV1_INTERVAL"])
    inv2_obs = get_inverter_values_observable(inverter2, env["INV2_INTERVAL"])

    log_observable_to_influx_db(get_combined_observable(
        [sdm_obs, inv1_obs, inv2_obs]), influx_params)


if __name__ == "__main__":
    main()
