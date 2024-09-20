import yaml
import json
import logging
import os
import pprint
from .io import simple_load, write_df


class MultiConfigLoaderMixin:
    """
    A mixin to extend DataSource for loading multiple configurations
    based on years and magnet polarities specified in the configuration.
    """

    def load_multiple_configs_from_config(self, data_type, **kwargs):
        """Load multiple configurations for the years and magnet polarities specified in the config."""
        years = self.config.get("years", [])
        magpols = self.config.get("magpols", [])
        if not years or not magpols:
            self.logger.error(
                "No years or magnet polarities specified in the configuration."
            )
            return

        for year in years:
            for magpol in magpols:
                self.logger.info(
                    f"Loading config for Year: {year}, Magnet Polarity: {magpol}"
                )
                try:
                    # Load the configuration for each year and magpol
                    self.run_simple_load(year, magpol, data_type, **kwargs)
                except Exception as e:
                    self.logger.error(
                        f"Failed to load config for Year: {year}, Magpol: {magpol}: {e}"
                    )


class DataWritingMixin:
    """
    A mixin to extend DataSource for writing data to a ROOT file.
    """

    def write_multiple_files_from_config(self, output_dir, data_type, **kwargs):
        """
        Load and write data for the years and magnet polarities specified in the configuration.

        Parameters:
        -----------
        output_dir : str
            The base directory where output files will be written.
        data_type : str
            The data type (e.g., 'data' or 'mc') to process.
        """
        years = self.config.get("years", [])
        magpols = self.config.get("magpols", [])
        if not years or not magpols:
            self.logger.error(
                "No years or magnet polarities specified in the configuration."
            )
            return

        for year in years:
            for magpol in magpols:
                self.logger.info(f"Processing Year: {year}, Magnet Polarity: {magpol}")
                try:
                    # Load the data
                    ntuple_data = self.run_simple_load(
                        year, magpol, data_type, **kwargs
                    )

                    # Define the output file path
                    output_file = (
                        f"{output_dir}/{self.channel}_{data_type}_{magpol}_{year}.root"
                    )

                    # Write the data using write_df function
                    self.logger.info(f"Writing data to {output_file}")
                    write_df(
                        ntuple_data, path=output_file, key=None, treename="DecayTree"
                    )

                    self.logger.info(
                        f"Successfully wrote data for Year: {year}, Magnet Polarity: {magpol}"
                    )

                except Exception as e:
                    self.logger.error(
                        f"Failed to process Year: {year}, Magpol: {magpol}: {e}"
                    )


class DataSource(MultiConfigLoaderMixin, DataWritingMixin):
    """
    A class to handle sourcing of ntuple files from the user configuration and JSON remote data map.
    """

    def __init__(self, config_path, json_path, channel):
        self.config_path = config_path
        self.json_path = json_path
        self.channel = channel  # Store the channel to use its configuration
        self.config = self.load_config()
        self.file_map = self.load_file_map()

    def setup_logging(self, log_file):
        """Set up logging to both console and a log file."""
        # Create the logs directory if it doesn't exist
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        # Create a logger
        self.logger = logging.getLogger(self.channel)
        self.logger.setLevel(logging.DEBUG)

        # Create handlers for logging to both console and file
        console_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(log_file)

        # Set logging levels for both handlers
        console_handler.setLevel(logging.INFO)  # Print info and above to console
        file_handler.setLevel(logging.DEBUG)  # Log everything to file

        # Create a formatter and set it for both handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add the handlers to the logger
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
        self.logger.debug(
            f"Fetching file for year: {year}, magpol: {magpol}, data_type: {data_type}"
        )
        try:
            return self.file_map[year][data_type][magpol]
        except KeyError as e:
            self.logger.error(
                f"Invalid combination of year ({year}), magpol ({magpol}), or data_type ({data_type}): {e}"
            )
            raise

    def run_simple_load(self, year, magpol, data_type, **kwargs):
        """
        Source the appropriate ntuple file, key, tree, and pass it to the simple_load function.
        Also sets up the log file dynamically based on channel, year, magpol, and data_type.
        """
        # Dynamically set log file path based on channel, year, magpol, and data type
        log_file = f"logs/datasource/{self.channel}_{data_type}_{magpol}_{year}.log"
        self.setup_logging(log_file)

        ntuple_file = self.get_file(year, magpol, data_type)
        key, tree = self.get_key_tree()
        self.logger.info(
            f"Running simple_load with file: {ntuple_file}, key: {key}, tree: {tree}"
        )

        # Call the simple_load function, passing key, tree, and any additional arguments.
        simple_load(ntuple_file, key=key, tree=tree, **kwargs)

        # Generate a report after loading
        self.generate_report(year, data_type, magpol)

    def generate_report(self, year, data_type, magpol):
        """Generate a nicely formatted report after the file is loaded."""
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
        self.logger.info(report)  # Log the report
        print(report)  # Also print the report to the console

    @property
    def available_years(self):
        """Return the available years from the configuration."""
        return self.config.get("years", [])

    @property
    def available_magpols(self):
        """Return the available magnet polarities from the configuration."""
        return self.config.get("magpols", [])

    @property
    def available_data_types(self):
        """Return the available data types from the configuration."""
        return self.config.get("data_types", [])

    def __repr__(self):
        """A string representation for debugging purposes with a nicely formatted channel overview."""
        channel_config = self.config["channel"][self.channel]
        repr_str = f"\nDataSource for Channel: {self.channel}\n"
        repr_str += "Configuration:\n"
        repr_str += pprint.pformat(channel_config, indent=4)
        return repr_str


class DataSourceNewChannel(DataSource):
    """
    Specialized class for a new channel data source. It can override methods if needed.
    """

    def run_simple_load(self, year, magpol, data_type, **kwargs):
        """Custom behavior for the new channel's simple load can be added here."""
        self.logger.info(f"Running simple_load for the new channel: {self.channel}")

        # Call the parent method with the same parameters
        super().run_simple_load(year, magpol, data_type, **kwargs)

        # Optionally generate additional custom reports, analytics, or logs for this channel
        self.logger.info(
            f"Completed run for the new channel with {year}, {magpol}, {data_type}."
        )


class DataSourceFactory:
    """
    Factory class to create different DataSource instances based on the channel.
    """

    @staticmethod
    def create_data_source(channel, config_path, json_path):
        """Factory method to create the appropriate DataSource based on the channel name."""
        if channel == "bu2kmumu":
            return DataSource(config_path, json_path, channel)
        # custom functionalities may be added by overriding the DataSource class as follows:
        # elif channel == "new_channel":
        #     return DataSourceNewChannel(config_path, json_path, channel)
        else:
            raise ValueError(f"Unknown channel: {channel}")


# # Example usage:
# if __name__ == "__main__":
#     # Example configuration paths and channel
#     config_path = "config/main.yml"
#     json_path = "data/eos_butojpsik_run12.json"

#     # Use the factory to create the appropriate DataSource instance
#     data_source = DataSourceFactory.create_data_source(
#         channel="bu2kmumu",
#         config_path=config_path,
#         json_path=json_path
#     )

#     # Print the data source details (channel configuration)
#     print(data_source)

#     # Run the simple_load function and generate a report
#     data_source.run_simple_load(year="2011", magpol="MagUp", data_type="data")

#     # Example with the new channel
#     new_channel_source = DataSourceFactory.create_data_source(
#         channel="new_channel",
#         config_path=config_path,
#         json_path=json_path
#     )

#     new_channel_source.run_simple_load(year="2015", magpol="MagDown", data_type="mc")
