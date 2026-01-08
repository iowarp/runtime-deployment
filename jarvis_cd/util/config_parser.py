"""
Configuration file parsers for JSON and YAML files.
Provides simple load/save interfaces for common configuration file formats.
"""

import json
import yaml
from pathlib import Path
from typing import Any, Dict, Union


class JsonFile:
    """
    A simple JSON file handler with load and save methods.

    Usage:
        # Save data to JSON file
        JsonFile('/path/to/file.json').save({'key': 'value'})

        # Load data from JSON file
        data = JsonFile('/path/to/file.json').load()
    """

    def __init__(self, path: Union[str, Path]):
        """
        Initialize JsonFile with a path.

        :param path: Path to the JSON file
        """
        self.path = Path(path)

    def load(self) -> Dict[str, Any]:
        """
        Load and parse the JSON file.

        :return: Parsed JSON content as a dictionary
        :raises FileNotFoundError: If the file doesn't exist
        :raises json.JSONDecodeError: If the file contains invalid JSON
        """
        with open(self.path, 'r') as f:
            return json.load(f)

    def save(self, data: Dict[str, Any], indent: int = 2) -> None:
        """
        Save data to the JSON file.

        :param data: Dictionary to save as JSON
        :param indent: Indentation level for pretty printing (default: 2)
        """
        # Ensure parent directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.path, 'w') as f:
            json.dump(data, f, indent=indent)


class YamlFile:
    """
    A simple YAML file handler with load and save methods.

    Usage:
        # Save data to YAML file
        YamlFile('/path/to/file.yaml').save({'key': 'value'})

        # Load data from YAML file
        data = YamlFile('/path/to/file.yaml').load()
    """

    def __init__(self, path: Union[str, Path]):
        """
        Initialize YamlFile with a path.

        :param path: Path to the YAML file
        """
        self.path = Path(path)

    def load(self) -> Dict[str, Any]:
        """
        Load and parse the YAML file.

        :return: Parsed YAML content as a dictionary
        :raises FileNotFoundError: If the file doesn't exist
        :raises yaml.YAMLError: If the file contains invalid YAML
        """
        with open(self.path, 'r') as f:
            return yaml.safe_load(f) or {}

    def save(self, data: Dict[str, Any], default_flow_style: bool = False) -> None:
        """
        Save data to the YAML file.

        :param data: Dictionary to save as YAML
        :param default_flow_style: If True, use flow style (inline) formatting
        """
        # Ensure parent directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.path, 'w') as f:
            yaml.dump(data, f, default_flow_style=default_flow_style)
