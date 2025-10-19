
from enum import Enum

class SensorType(Enum):
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    PRESSURE = "pressure"
    LIGHT = "light"


class ErrorType(Enum):
    NONE = 0
    CORRUPTED_DATA = 1
    DUPLICATE = 2
    OUT_OF_RANGE = 3
    MISSING_FIELD = 4
    NETWORK_TIMEOUT = 5