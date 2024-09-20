__version__ = "0.1.0"
import os
from .utils import *

DEFAULT_CONFIG_PATH = os.getenv("TREX_CONFIG_PATH", "config/main.yaml")
