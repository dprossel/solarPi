import os
import sys


def get_environment_variables(variables: list):
    try:
        return {var: os.environ[var] for var in variables}
    except KeyError as err:
        print("Environment variable(s) not set!")
        print("Expected these variables: " + str(variables))
        print("But found only these: " + str(list(os.environ.keys())))
        print("Exiting...")
        sys.exit(1)
