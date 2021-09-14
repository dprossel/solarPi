#!/usr/bin/env python3
from solarpi import get_sdm_energy_values_observable, log_observable_to_influx_db


def main():
    log_observable_to_influx_db(get_sdm_energy_values_observable(1))


if __name__ == "__main__":
    main()
