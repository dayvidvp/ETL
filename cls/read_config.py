import yaml
from loguru import logger
import os


@logger.catch
def read_config(config_file: str):
    config_path = os.path.abspath(os.curdir)
    config_dir = "config"
    config_file = os.path.join(
        config_path + os.sep + config_dir + os.sep + config_file
    )

    with open(config_file, "r") as stream:
        try:
            parsed_yaml = yaml.safe_load(stream)
            return parsed_yaml
        except yaml.YAMLError as exc:
            logger.error(exc)
