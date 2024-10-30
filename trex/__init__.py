import os
from pathlib import Path
from trex.utils.config import ConfigLoader
from trex.utils.io import (
    oneshot_load_root,
    batched_load_root,
    update_write_df,
    write_df,
    load_hist,
)

# Define potential config and data directory paths, relative to the module and current directory
CONFIG_LOCATIONS = [
    os.path.join(
        Path(__file__).parents[1], "config", "main.yml"
    ),  # trex/config/main.yml relative to the trex module
    os.path.join(
        Path(__file__).parents[0], "config", "main.yml"
    ),  # config/main.yml relative to current directory
]

DATA_LOCATIONS = [
    os.path.join(
        Path(__file__).parents[1], "data"
    ),  # /data/ relative to the trex module
    os.path.join(
        Path(__file__).parents[0], "data"
    ),  # /data/ relative to current directory
]


def find_valid_path(locations, description):
    """Search for the first valid path in the given list of locations."""
    for path in locations:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"No {description} found in the expected locations.")


# Get the config path, either from the environment variable or by searching the default locations
DEFAULT_CONFIG_PATH = os.getenv(
    "TREX_CONFIG_PATH", find_valid_path(CONFIG_LOCATIONS, "configuration file")
)

# Get the data path, either from the environment variable or by searching the default locations
DEFAULT_DATA_PATH = os.getenv(
    "TREX_DATA_PATH", find_valid_path(DATA_LOCATIONS, "data directory")
)

__version__ = "0.1.0"
