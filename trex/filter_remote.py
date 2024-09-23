"""
Filter data on remote storage (eos) to produce more manageable files for local processing.
"""

from trex.utils.io import simple_load, read_config
from trex.utils.auth import kinit
from trex.utils.data_sources import DataSourceFactory

# based on config, read in and filter remote files
if __name__ == "__main__":

    # data
    for dtype in ("data", "mc"):
        data_source = DataSourceFactory.create_data_source(
            "butojpsik_mm"
        ).read_filter_write_file(
            year="2018",
            magpol="MU",
            data_type=dtype,
            output_dir="/ceph/submit/data/user/b/blaised/trex",
        )
