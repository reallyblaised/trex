import yaml
import json
from loguru import logger
from pathlib import Path
from trex.utils.io import batched_load_root, write_df
from trex.utils.auth import kinit
from trex.utils.config import ConfigLoader
from trex import DEFAULT_CONFIG_PATH, DEFAULT_DATA_PATH
from trex.utils.filter import FilterFactoryMixin
from typing import Union
import pandas as pd
import awkward as ak

class DataWriterMixin:
    """Mixin for writing data to a ROOT file."""
    
    @staticmethod
    def save(df: Union[pd.DataFrame, ak.Array], output_path: str, key: str = None, treename: str = "DecayTree") -> None:
        """Write the data to a ROOT file."""
        logger.info(f"Writing data to {output_path}")
        write_df(df, path=output_path, key=key, treename=treename)
        logger.info(f"Successfully wrote data to {output_path}")


class DataSource(FilterFactoryMixin, DataWriterMixin):
    """
    Class to perform standalone data loading per channel, year, data type, as instructed in the config YAML file.
    """

    def __init__(self, channel: str, json_path: str, config_loader=ConfigLoader, logfile="datasource.log"):
        self.logger = logger
        self.setup_logging(logfile)
        self._channel = channel
        self.config = config_loader(DEFAULT_CONFIG_PATH).config
        self.file_map = self.load_file_map(json_path)


    @property
    def channel(self) -> str:
        return self._channel

    def setup_logging(self, logfile: str) -> None:
        """Set up logging to both console and a log file."""
        logdir = Path("logs")
        logdir.mkdir(parents=True, exist_ok=True)

        # Logger setup with file rotation
        self.logger.add(
            logdir / logfile,
            rotation="10 MB",
            retention="7 days",
            level="INFO",
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name}:{function}:{line} - {message}"
        )

    def load_file_map(self, json_path: str) -> dict:
        """Load JSON file map."""
        self.logger.info(f"Loading file map: {json_path}")
        try:
            with open(json_path, "r") as file:
                self.logger.info(f"File map loaded successfully.")
                return json.load(file)
        except FileNotFoundError as e:
            self.logger.error(f"File map not found: {e}")
            raise

    def get_data_path(self, year: str, magpol: str, data_type: str) -> str:
        """Get the appropriate file path from the JSON database based on year, magpol, and data type."""
        self.logger.debug(f"Fetching file for year: {year}, magpol: {magpol}, data_type: {data_type}")
        try:
            self.logger.debug(f"Fetching file @ {self.file_map[year][data_type][magpol]}: {self.file_map[year][data_type][magpol]}")
            return self.file_map[year][data_type][magpol]
        except KeyError as e:
            self.logger.error(f"Invalid combination of year ({year}), magpol ({magpol}), or data_type ({data_type}): {e}")
            raise

    def _generate_report(self, year: str, data_type: str, magpol: str, selection_criteria: str) -> None:
            """Generate a report after loading."""
            channel_config = self.config["channel"][self.channel]
            boi = channel_config.get("boi", [])

            report = f"""
            ============ File Load Report ============
            Channel: {self.channel}
            Year: {year}
            Data Type: {data_type}
            Magnet Polarity: {magpol}
            -----------------------------------------
            Branches of Interest (BOI):
            {', '.join(boi)}

            Selection Criteria:
            {selection_criteria}
            =========================================
            """
            self.logger.info(report)
            print(report)

    def process_root_file(self, year: str, magpol: str, data_type: str, **kwargs) -> Union[pd.DataFrame, ak.Array]:
        """Load the ntuple file, pass it to the simple_load function, and handle binned fits."""
        ntuple_file = self.get_data_path(year, magpol, data_type)
        channel_config = self.config["channel"][self.channel]
        key = channel_config.get("key", None)
        tree = channel_config.get("tree", None)
        boi = channel_config.get("boi", [])
        aliases = channel_config.get("aliases", {})
        selection = channel_config.get("selection", {})
        max_events = self.config.get('max_events', None)

        # establish the selection criteria for filtering the sample upfront
        filter_string, itemised_criteria = self.generate_selection(year, data_type, magpol)
        
        self.logger.info(f"Running load for file: {ntuple_file}, key: {key}, data_type: {data_type}, max_events: {max_events}")
        df = batched_load_root(ntuple_file, key=key, tree=tree, branches=boi, cut=filter_string, max_events=max_events, aliases=aliases, **kwargs)

        if df is not None and len(df) > 0:
            self._generate_report(year, data_type, magpol, itemised_criteria)
            logger.info(f"Successfully loaded data for Year: {year}, Magpol: {magpol}, Data Type: {data_type}")
        if df is None: 
            self.logger.warning(f"No data loaded for Year: {year}, Magpol: {magpol}, Data Type: {data_type}")
        
        return df


class EosDataSource(DataSource):
    """
    A DataSource that uses Kerberos authentication for accessing files from EOS storage.
    """

    def __init__(self, channel: str, json_path: str, config_path=DEFAULT_CONFIG_PATH, logfile="eos_datasource.log"):
        super().__init__(channel, json_path, config_loader=ConfigLoader, logfile=logfile)
        self.authenticate()

    def authenticate(self) -> None:
        """Authenticate using Kerberos for accessing EOS files."""
        user_id = self.config.get("user_id")
        if not user_id:
            self.logger.error("User ID not specified in the configuration.")
            raise ValueError("User ID not specified in the configuration.")


class DataSourceFactory:
    """
    Factory class to create appropriate DataSource instances based on the channel.
    """

    @staticmethod
    def create_data_source(channel: str, data_dir_path: str = DEFAULT_DATA_PATH) -> DataSource:
        """Create the appropriate DataSource instance based on the channel name."""
        match channel:
            case "butojpsik_mm":
                json_path = Path(data_dir_path) / "eos_butojpsik_run12.json"
                return EosDataSource(channel=channel, json_path=str(json_path))
            case _:
                raise ValueError(f"Unknown channel: {channel}")


# Usage Example:
if __name__ == "__main__":
    factory = DataSourceFactory()
    ds = factory.create_data_source("butojpsik_mm")
    df = ds.process_root_file(year="2016", magpol="MU", data_type="mc")