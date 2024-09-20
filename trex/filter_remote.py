"""
Filter data on remote storage (eos) to produce more manageable files for local processing.
"""

from trex.utils.io import simple_load, read_config
from trex.utils.auth import kinit
from trex.utils.data_sources import 


# initialise the kerberos ticket
kinit(read_config(path="config/main.yml", key="user_id"))

# based on config, read in and filter remote files
if __name__ == "__main__":

    json_path = "data/eos_butjpsi_run12.json"
    
    data_source = 