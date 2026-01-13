from pathlib import Path
from logging_utils import create_run_logger

def main():
    logger = create_run_logger("cdc_writer", "test_run_001", Path("./data/logs"))
    logger.info("hello from logger")
    logger.warning("this is a warning")
    logger.error("this is an error")

if __name__ == "__main__":
    main()
