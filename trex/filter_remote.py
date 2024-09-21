"""
Filter data on remote storage (eos) to produce more manageable files for local processing.
"""

from trex.utils.io import simple_load, read_config
from trex.utils.auth import kinit
from trex.utils.data_sources import DataSourceFactory

# based on config, read in and filter remote files
if __name__ == "__main__":

    data_source = DataSourceFactory.create_data_source("butojpsik_mm").run_simple_load(
        year="2018", magpol="MU", data_type="data"
    )
