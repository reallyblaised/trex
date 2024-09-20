"""
Filter data on remote storage (eos) to produce more manageable files for local processing.
"""

from trex.utils import simple_load, read_config, kinit
from pathlib import Path

# initialise the kerberos ticket
kinit(read_config(path="config/main.yml", key="user_id"))

# based on config, read in and filter remote files
