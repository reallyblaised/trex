"""
Filter data on remote storage (eos) to produce more manageable files for local processing.
"""

from trex.utils.data_sources import DataSourceFactory
from pathlib import Path
from trex.utils.io import write_df

# based on config, read in and filter remote files
if __name__ == "__main__":

    for year in ("2018",):
        for magpol in ("MU", "MD"):
            for dtype in ("mc", "data"):
                data_source = DataSourceFactory.create_data_source(
                    "butojpsik_mm"
                ).process_root_file(
                    year=year,
                    magpol=magpol,
                    data_type=dtype,
                )
                output_dir = Path(
                    f"/ceph/submit/data/user/b/blaised/trex4ddmisid/{year}/{magpol}/{dtype}"
                )
                output_dir.mkdir(parents=True, exist_ok=True)
                write_df(
                    data_source,
                    f"{output_dir}/butojpsik_mm_{year}_{magpol}_{dtype}.root",
                )
