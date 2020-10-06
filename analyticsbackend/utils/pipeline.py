import json
import os
from analyticsbackend.utils.databricks import get_path_for_python_ops


def _is_relative_path(target_value):
    """
    Check if :target_value is a relative path
    """
    return isinstance(target_value, str) and target_value.startswith("./")


def _relative_to_abs_path(working_directory: str, relative_path: str):
    """
    Merges :working_directory path and :relative_path together
    """
    return working_directory + relative_path[1:]


def read_config_file(working_directory: str, config_file_name: str) -> dict:
    """
    Read and parse the configuration file into a dictionary usable inside a pipeline.
    It changes all the relative paths to absolute paths based on :working_directory.
    Important! It does not go through nested objects or list.
    """
    config_file_path = os.path.join(working_directory, config_file_name)
    with open(get_path_for_python_ops(config_file_path)) as config_file:
        config_dictionary = json.load(config_file)
        for k in config_dictionary.keys():
            if _is_relative_path(config_dictionary[k]):
                config_dictionary[k] = _relative_to_abs_path(
                    working_directory, config_dictionary[k]
                )
        return config_dictionary
