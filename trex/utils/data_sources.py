import yaml
import json
from .io import simple_load


class DataSource:
    """
    A class to handle sourcing of ntuple files from the user configuration and JSON remote data map.
    """

    def __init__(self, config_path, json_path):
        self.config_path = config_path
        self.json_path = json_path
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

    def get_file(self, year: str, magpol: str, data_type: str):
        """
        Get the appropriate file path from the JSON data based on the year, magnet polarity,
        and data type (data or MC).
        """
        try:
            return self.file_map[year][data_type][magpol]
        except KeyError as e:
            raise ValueError(
                f"Invalid combination of year ({year}), magpol ({magpol}), or data_type ({data_type}): {e}"
            )

    def run_simple_load(self, year, magpol, data_type):
        """
        Source the appropriate ntuple file and pass it to the simple_load function.
        """
        ntuple_file = self.get_file(year, magpol, data_type)
        simple_load(ntuple_file)  # Call the simple_load function from trex.utils

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
        """A string representation for debugging purposes."""
        return (
            f"DataSource(config_path='{self.config_path}', json_path='{self.json_path}', "
            f"available_years={self.available_years}, available_magpols={self.available_magpols}, "
            f"available_data_types={self.available_data_types})"
        )


# Example subclass for a new channel
class DataSourceNewChannel(DataSource):
    """
    Specialized class for new channel data source. It can override methods if needed.
    """

    def run_simple_load(self, year, magpol, data_type):
        # You can customize the behavior for this new channel here
        print(
            f"Running simple_load for the new channel with {year}, {magpol}, {data_type}"
        )
        super().run_simple_load(year, magpol, data_type)


class DataSourceFactory:
    """
    Factory class to create different DataSource instances based on the channel.
    """

    @staticmethod
    def create_data_source(channel, config_path, json_path):
        if channel == "bu2kmumu":
            return DataSource(config_path, json_path)
        elif channel == "new_channel":
            return DataSourceNewChannel(
                config_path, json_path
            )  # Specialized DataSource for a new channel
        else:
            raise ValueError(f"Unknown channel: {channel}")
