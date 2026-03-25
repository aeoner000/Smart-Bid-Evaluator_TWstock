
from pathlib import Path
import yaml
import logging

logger = logging.getLogger(__name__)

# Define the root path of the project
ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "config.yaml"

def load_config() -> dict:
    """
    Loads the configuration from config.yaml, handling potential errors.
    """
    if not CONFIG_PATH.exists():
        logger.error(f"Configuration file not found at: {CONFIG_PATH}")
        raise FileNotFoundError(f"config.yaml not found at {CONFIG_PATH}")

    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            # Safely load the YAML file, return empty dict if it's empty
            config_data = yaml.safe_load(f) or {}
            logger.info("Configuration file loaded successfully.")
            return config_data
    except yaml.YAMLError as e:
        logger.error(f"Error parsing the YAML file: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading the config: {e}")
        raise

# Load the configuration once and export it for other modules to use.
# This is the single source of truth for all configurations.
config = load_config()
