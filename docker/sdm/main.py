#!/usr/bin/env python3
from solarpi import get_sdm_energy_values_observable, log_observable_to_influx_db, InfluxDbParams
from solarpi.utils import get_environment_variables
from sdm_modbus import SDM630


def main():
    env = get_environment_variables(
        ["INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG", "INFLUXDB_BUCKET", "SERIAL_DEVICE", "SERIAL_BAUDRATE", "SDM_INTERVAL"])

    influx_params = InfluxDbParams(url=env["INFLUXDB_URL"], token=env["INFLUXDB_TOKEN"],
                                   organisation=env["INFLUXDB_ORG"], bucket=env["INFLUXDB_BUCKET"])

    device = SDM630(device=env["SERIAL_DEVICE"], baud=int(env["SERIAL_BAUDRATE"]), parity="N")
    log_observable_to_influx_db(get_sdm_energy_values_observable(
        device, float(env["SDM_INTERVAL"])), influx_params)


if __name__ == "__main__":
    main()