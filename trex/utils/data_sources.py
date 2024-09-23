import yaml
import json
import logging
import os
import pprint
from .io import load_root, write_df
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
                    self.load_root_file(year, magpol, data_type, **kwargs)
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
                    ntuple_data = self.load_root_file(year, magpol, data_type, **kwargs)

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

    def read_filter_write_file(self, year, magpol, data_type, output_dir, **kwargs):
        """
        Write a single file from the loaded ntuple data.

        Parameters:
        -----------
        year : str
            The year of the data to process.
        magpol : str
            The magnet polarity.
        data_type : str
            The data type (e.g., 'data' or 'mc').
        output_dir : str
            The directory where the file will be written.
        """
        self.logger.info(f"Processing single file for Year: {year}, Magnet Polarity: {magpol}, Data Type: {data_type}")

        try:
            # Load the data
            ntuple_data = self.load_root_file(year, magpol, data_type, **kwargs)

            # Define the output file path
            if not output_dir:
                self.logger.error("Output directory not specified.")
                raise ValueError("Output directory not specified.")
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

    @staticmethod
    def parse_data_selection(selection: dict) -> str:
        """Parse the selection criteria from the YAML config file, excluding truth_matching."""
        return " & ".join([v for k, v in selection.items() if k != "truth_matching"])
    
    @staticmethod
    def parse_mc_selection(selection: dict) -> str:
        """Parse the selection criteria from the YAML config file."""
        return " & ".join([v for v in selection.values()]) 

    def get_read_params(self, data_type: str):
        """Get the key, tree, branches of interest, and selection criteria for the selected channel."""
        channel_config = self.config["channel"][self.channel]
        key = channel_config.get("key")
        tree = channel_config.get("tree")
        boi = channel_config.get("boi")
        sel = channel_config.get("selection")

        if not key or not tree or not boi or not sel:
            raise ValueError(f"Missing necessary parameters in the channel configuration.")

        # process the selection and branches, ignoring truth conditions for data
        match data_type:
            case "data":
                sel = self.parse_data_selection(sel)
                boi = [c for c in boi if "TRUEID" not in c and "BKGCAT" not in c]  # Filter for data selection
            case "mc":
                sel = self.parse_mc_selection(sel)
            case _:
                raise ValueError(f"Invalid data type: {data_type} provided to get_read_params")

        self.logger.debug(f"Received input loading coordinates: key={key}, tree={tree}, data_type={data_type}, boi={boi}, sel={sel}")
        return key, tree, boi, sel

    def get_file(self, year: str, magpol: str, data_type: str):
        """Get the appropriate file path from the JSON database based on the year, magnet polarity, and data type."""
        self.logger.debug(f"Fetching file for year: {year}, magpol: {magpol}, data_type: {data_type}")
        try:
            return self.file_map[year][data_type][magpol]
        except KeyError as e:
            self.logger.error(f"Invalid combination of year ({year}), magpol ({magpol}), or data_type ({data_type}): {e}")
            raise

    def load_root_file(self, year, magpol, data_type, **kwargs):
        """Source ntuple file, key, tree, and pass it to the simple_load function."""
        ntuple_file = self.get_file(year, magpol, data_type)
        key, tree, boi, sel = self.get_read_params(data_type)
        max_events = self.config.get('max_events', None) # curtail the number of events to load in testing
        self.logger.info(f"Running simple_load with file: {ntuple_file}, key: {key}, data_type: {data_type}, max_events: {max_events}")

        # batched load to handle large files - by default read into akward arrays
        df = load_root(ntuple_file, key=key, tree=tree, branches=boi, cut=sel, max_events=max_events, **kwargs)
        
        if len(df) > 0:
            self.generate_report(year, data_type, magpol)
        else:
            self.logger.warning(f"No data loaded for Year: {year}, Magpol: {magpol}, Data Type: {data_type}")

        return df

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
        {', '.join(boi)} [excluding mc-truth branches if data]

        Selection Criteria:
        Common Criteria: {selection.get('common_criteria', 'N/A')}
        Global TIS: {selection.get('global_tis', 'N/A')}
        Muon TOS: {selection.get('muon_tos', 'N/A')}
        Truth Matching: {selection.get('truth_matching', 'N/A')} [excluded if data]
        =========================================
        """
        self.logger.debug(report)
        print(report)


class EosDataSource(DataSource):
    """
    Integrate Kerberos authentication to source files from remote EOS location.
    """

    def __init__(self, json_path, channel, config_path=DEFAULT_CONFIG_PATH):
        # Call the parent class (DataSource) initializer
        super().__init__(json_path, channel, config_path)

        # Now handle the Kerberos authentication
        self.authenticate()

    def authenticate(self):
        """Authenticate using Kerberos for accessing EOS files."""
        user_id = self.config.get("user_id")
        if not user_id:
            self.logger.error("User ID not specified in the configuration.")
            raise ValueError("User ID not specified in the configuration.")

        kinit(user_id)  # error handling 



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
