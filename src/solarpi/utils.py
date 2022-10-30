import os
import sys
from solarpi import InfluxDbParams, MqttParams

def get_environment_variables(variables: list) -> dict:
    try:
        return {var: os.environ[var] for var in variables}
    except KeyError as err:
        print("Environment variable(s) not set!")
        print("Expected these variables: " + str(variables))
        print("But found only these: " + str(list(os.environ.keys())))
        print("Exiting...")
        sys.exit(1)

def get_influxdb_params_from_env() -> InfluxDbParams:
    return InfluxDbParams(*(get_environment_variables(["INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG", "INFLUXDB_BUCKET"]).values()))

def get_mqtt_params_from_env() -> MqttParams:
    return MqttParams(*(get_environment_variables(["MQTT_BROKER", "MQTT_PORT", "MQTT_CLIENTID"]).values()))
