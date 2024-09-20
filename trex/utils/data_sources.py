import yaml
import json
from .io import simple_load


class DataSource:
    """
    A class to handle sourcing of ntuple files from the user configuration and JSON remote data map.
    """

    def __init__(self, config_path, json_path, channel):
        self.config_path = config_path
        self.json_path = json_path
        self.channel = channel  # Store the channel to use its configuration
        self.config = self.load_config()
        self.file_map = self.load_file_map()

    def load_config(self):
        """Load YAML config file."""
        with open(self.config_path, "r") as file:
            return yaml.safe_load(file)

    def load_file_map(self):
        """Load JSON file map."""
        with open(self.json_path, "r") as file:
            return json.load(file)

    def get_key_tree(self):
        """
        Get the key and tree coordinates from the YAML config file for the selected channel.
        """
        channel_config = self.config["channel"][self.channel]
        key = channel_config.get("key", "N/A")
        tree = channel_config.get("tree", "N/A")
        return key, tree

    def get_file(self, year: str, magpol: str, data_type: str):
        """
        Get the appropriate file path from the JSON database based on the year, magnet polarity,
        and data type (data or MC).
        """
        try:
            return self.file_map[year][data_type][magpol]
        except KeyError as e:
            raise ValueError(
                f"Invalid combination of year ({year}), magpol ({magpol}), or data_type ({data_type}): {e}"
            )

    def run_simple_load(self, year, magpol, data_type, **kwargs):
        """
        Source the appropriate ntuple file, key, tree, and pass it to the simple_load function.
        """
        ntuple_file = self.get_file(year, magpol, data_type)
        key, tree = self.get_key_tree()
        print(f"Running simple_load with file: {ntuple_file}, key: {key}, tree: {tree}")

        # Call the simple_load function, passing key, tree, and any additional arguments.
        simple_load(ntuple_file, key=key, tree=tree, **kwargs)

        # Generate a report after loading
        self.generate_report(year, data_type, magpol)

    def generate_report(self, year, data_type, magpol):
        """
        Generate a nicely formatted report after the file is loaded.
        """
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
        print(report)

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
        repr_str += pprint.pformat(
            channel_config, indent=4
        )  # Nicely format the channel config
        repr_str += "\n"
        return repr_str


class DataSourceNewChannel(DataSource):
    """
    Specialized class for a new channel data source. It can override methods if needed.
    """

    def run_simple_load(self, year, magpol, data_type, **kwargs):
        """
        Custom behavior for the new channel's simple load can be added here.
        """
        # Customize anything specific to the new channel, if needed.
        print(f"Running simple_load for the new channel: {self.channel}")

        # Call the parent method with the same parameters
        super().run_simple_load(year, magpol, data_type, **kwargs)

        # Optionally generate additional custom reports, analytics, or logs for this channel
        print(f"Completed run for the new channel with {year}, {magpol}, {data_type}.")


class DataSourceFactory:
    """
    Factory class to create different DataSource instances based on the channel.
    """

    @staticmethod
    def create_data_source(channel, config_path, json_path):
        """
        Factory method to create the appropriate DataSource based on the channel name.
        """
        if channel == "bu2kmumu":
            return DataSource(config_path, json_path, channel)
        elif channel == "new_channel":
            # Create an instance of the specialized DataSource subclass for a new channel
            return DataSourceNewChannel(config_path, json_path, channel)
        else:
            raise ValueError(f"Unknown channel: {channel}")
