import yaml
import json
import logging
import os
import pprint
from .io import simple_load, write_df
from .auth import kinit
from trex import DEFAULT_CONFIG_PATH, DEFAULT_DATA_PATH


class MultiConfigLoaderMixin:
    """Mixin for loading configurations based on years and magnet polarities."""

    def load_multiple_configs_from_config(self, data_type, **kwargs):
        """Load multiple configurations for the years and magnet polarities specified in the config."""
        years = self.config.get("years", [])
        magpols = self.config.get("magpols", [])
        if not years or not magpols:
            self.logger.error("No years or magnet polarities specified in the configuration.")
            return

        for year in years:
            for magpol in magpols:
                self.logger.info(f"Loading config for Year: {year}, Magnet Polarity: {magpol}")
                try:
                    # Load the configuration for each year and magpol
                    self.run_simple_load(year, magpol, data_type, **kwargs)
                except Exception as e:
                    self.logger.error(f"Failed to load config for Year: {year}, Magpol: {magpol}: {e}")


class DataWritingMixin:
    """Mixin for writing data to a ROOT file."""

    def write_multiple_files_from_config(self, data_type, **kwargs):
        """
        Load and write data for the years and magnet polarities specified in the configuration.
        """
        years = self.config.get("years", [])
        magpols = self.config.get("magpols", [])
        output_dir = self.config.get("output_storage", None)
        if not years or not magpols:
            self.logger.error("No years or magnet polarities specified in the configuration.")
            return

        for year in years:
            for magpol in magpols:
                self.logger.info(f"Processing Year: {year}, Magnet Polarity: {magpol}")
                try:
                    # Load the data
                    ntuple_data = self.run_simple_load(year, magpol, data_type, **kwargs)

                    # Define the output file path
                    if not output_dir:
                        self.logger.error("Output directory not specified in the configuration.")
                        raise ValueError("Output directory not specified in the configuration.")
                    output_file = f"{output_dir}/{self.channel}_{data_type}_{magpol}_{year}.root"

                    # Write the data using write_df function
                    self.logger.info(f"Writing data to {output_file}")
                    write_df(ntuple_data, path=output_file, key=None, treename="DecayTree")

                    self.logger.info(f"Successfully wrote data for Year: {year}, Magnet Polarity: {magpol}")
                except Exception as e:
                    self.logger.error(f"Failed to process Year: {year}, Magpol: {magpol}: {e}")


class DataSource(MultiConfigLoaderMixin, DataWritingMixin):
    """
    A class to handle sourcing of ntuple files from the user configuration and JSON remote data map.
    """

    def __init__(self, json_path, channel, config_path=DEFAULT_CONFIG_PATH):
        self.config_path = config_path
        self.json_path = json_path
        self.channel = channel

        # Set up logging first, as we are using logger in other methods
        log_file = f"logs/datasource/{self.channel}.log"
        self.setup_logging(log_file)

        # Now that logging is set up, you can load config and file map
        self.config = self.load_config()
        self.file_map = self.load_file_map()


    def setup_logging(self, log_file):
        """Set up logging to both console and a log file."""
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        self.logger = logging.getLogger(self.channel)
        self.logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(log_file)

        console_handler.setLevel(logging.INFO)
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def load_config(self):
        """Load YAML config file."""
        self.logger.info(f"Loading configuration from {self.config_path}")
        try:
            with open(self.config_path, "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError as e:
            self.logger.error(f"Config file not found: {e}")
            raise

    def load_file_map(self):
        """Load JSON file map."""
        self.logger.info(f"Loading file map from {self.json_path}")
        try:
            with open(self.json_path, "r") as file:
                return json.load(file)
        except FileNotFoundError as e:
            self.logger.error(f"File map not found: {e}")
            raise

    def get_key_tree(self):
        """Get the key and tree coordinates from the YAML config file for the selected channel."""
        channel_config = self.config["channel"][self.channel]
        key = channel_config.get("key", "N/A")
        tree = channel_config.get("tree", "N/A")
        self.logger.info(f"Retrieved key: {key}, tree: {tree}")
        return key, tree

    def get_file(self, year: str, magpol: str, data_type: str):
        """Get the appropriate file path from the JSON database based on the year, magnet polarity, and data type."""
        self.logger.debug(f"Fetching file for year: {year}, magpol: {magpol}, data_type: {data_type}")
        try:
            return self.file_map[year][data_type][magpol]
        except KeyError as e:
            self.logger.error(f"Invalid combination of year ({year}), magpol ({magpol}), or data_type ({data_type}): {e}")
            raise

    def run_simple_load(self, year, magpol, data_type, **kwargs):
        """Source ntuple file, key, tree, and pass it to the simple_load function."""
        log_file = f"logs/datasource/{self.channel}_{data_type}_{magpol}_{year}.log"
        self.setup_logging(log_file)

        ntuple_file = self.get_file(year, magpol, data_type)
        key, tree = self.get_key_tree()
        self.logger.info(f"Running simple_load with file: {ntuple_file}, key: {key}, tree: {tree}")

        simple_load(ntuple_file, key=key, tree=tree, **kwargs)
        self.generate_report(year, data_type, magpol)

    def generate_report(self, year, data_type, magpol):
        """Generate a report after loading."""
        channel_config = self.config["channel"][self.channel]
        boi = channel_config.get("boi", [])
        selection = channel_config.get("selection", {})

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
        Common Criteria: {selection.get('common_criteria', 'N/A')}
        Global TIS: {selection.get('global_tis', 'N/A')}
        Muon TOS: {selection.get('muon_tos', 'N/A')}
        Truth Matching: {selection.get('truth_matching', 'N/A')}
        =========================================
        """
        self.logger.info(report)
        print(report)


class EosDataSource(DataSource):
    """
    Specialized class for a new channel data source. It can override methods if needed.
    """

    def run_simple_load(self, year, magpol, data_type, **kwargs):
        """Souce file from remote eos location after initialising the CERN Kerberos ticket."""

        user_id = self.config["user_id"]
        try:
            kinit(user_id)
            self.logger.info(f"Successfully authenticated as {user_id}.")
        except Exception as e:
            self.logger.error(f"Kerberos authentication failed for user {user_id}: {e}")
            raise ValueError(f"Kerberos authentication failed for user {user_id}: {e}")

        self.logger.info(f"Running simple_load for the new channel: {self.channel}")

        super().run_simple_load(year, magpol, data_type, **kwargs)


class DataSourceFactory:
    """
    Factory class to create different DataSource instances based on the channel.
    """

    @staticmethod
    def create_data_source(channel, data_dir_path = DEFAULT_DATA_PATH):
        """Factory method to create the appropriate DataSource based on the channel name."""
        match channel:
            case "butojpsik_mm":
                json_path = os.getenv('EOS_JSON_PATH', f"{data_dir_path}/eos_butojpsik_run12.json")
                return EosDataSource(
                    json_path=json_path, channel="butojpsik_mm"
                )
            case _:
                raise ValueError(f"Unknown channel: {channel}")

    # # Example usage:
    # if __name__ == "__main__":

    #     # Example configuration paths and channel
    #     config_path = "config/main.yml"
    #     json_path = "data/eos_butojpsik_run12.json"

    #     # Use the factory to create the appropriate DataSource instance
    #     data_source = DataSourceFactory.create_data_source(
    #         channel="bu2kmumu", config_path=config_path, json_path=json_path
    #     )

    #     # Print the data source details (channel configuration)
    #     print(data_source)

    #     # Load multiple configurations based on the years and magnet polarities in the config
    #     data_source.load_multiple_configs_from_config(data_type="data")

    #     # Write multiple files based on the loaded configurations and write them to a ROOT file
    #     output_dir = "/path/to/output/directory"
    #     data_source.write_multiple_files_from_config(
    #         output_dir=output_dir, data_type="data"
    #     )
