import logging
import os
from logging.handlers import RotatingFileHandler

def init_logger(log_dir="logs", log_name="app.log"):
    """Initializes the global logger configuration."""
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_name)

    # Create a basic configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            # RotatingFileHandler: Automatically rotates logs when a file reaches 5MB, keeping 5 old files.
            RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    # Get the root logger and set its level. This ensures all child loggers inherit the level.
    # This is more robust than relying on the basicConfig level alone.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Log the initialization of the logging system itself.
    # We get a specific logger for this file to follow best practices.
    logger = logging.getLogger(__name__)
    logger.info("Logging system initialized. Outputting to: %s", log_path)

# init_logger is not called here; it should be called by the application's entry point.
