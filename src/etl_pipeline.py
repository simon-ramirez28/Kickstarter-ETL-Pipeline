import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
import sys

# --- LOGGER CONFIGURATION ---
LOG_FILE = "logs/etl_pipeline.log"
# Ensure the 'logs' folder exists
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# 1. Create the logger object
logger = logging.getLogger('KickstarterETL')
logger.setLevel(logging.INFO) # Minimum logging level (INFO, WARNING, ERROR, DEBUG)

# 2. Configure the log message format
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 3. Handler to write to the file (Rotation to avoid giant files)
# The file will rotate at 5MB and keep 2 backup files.
file_handler = RotatingFileHandler(
    LOG_FILE, 
    maxBytes=5*1024*1024, 
    backupCount=2,
    encoding='utf-8'
)
file_handler.setFormatter(formatter)

# 4. Handler to show logs on the console (sys.stdout)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

# 5. Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
# ---------------------------------

# Define the path to the data file
FILE_PATH = "data/raw/ks-projects-201801.csv"

def extract_data(file_path: str) -> pd.DataFrame:
    """
    Extracts (reads) the Kickstarter dataset from the CSV file.
    """
    logger.info("Extraction started.")
    logger.info(f"Attempting to read the file: {file_path}")
    try:
        # Here we use the encoding that works best for your data
        df = pd.read_csv(file_path, encoding='utf-8') 
        logger.info("Extraction completed successfully.")
        logger.info(f"DataFrame loaded with {df.shape[0]} rows and {df.shape[1]} columns.")
        return df
    except FileNotFoundError:
        # A critical error is logged if the main file is not found
        logger.critical(f"CRITICAL Error: The file was not found at {file_path}. Terminating execution.")
        return None
    except Exception as e:
        # An error is logged if any other issue occurs during reading
        logger.error(f"An unexpected error occurred during extraction: {e}", exc_info=True)
        return None

def inspect_data(df: pd.DataFrame):
    """
    Performs an initial inspection of the DataFrame and logs the information.
    """
    if df is not None:
        logger.info("--- Starting Initial Data Inspection ---")
        
        # Log the first rows and data types at DEBUG/INFO level
        logger.debug("First 5 rows:\n" + str(df.head()))
        logger.info("\nData types of columns:\n" + str(df.dtypes))
        
        # Counting states is useful for data quality
        state_counts = df['state'].value_counts()
        logger.info(f"Count of unique values in 'state':\n{state_counts.to_string()}")
        
        logger.info("Initial data inspection completed.")


if __name__ == "__main__":
    logger.info("==============================================")
    logger.info("START OF KICKSTARTER ETL PIPELINE")
    
    kickstarter_df = extract_data(FILE_PATH)
    
    if kickstarter_df is not None:
        inspect_data(kickstarter_df)
    
    logger.info("END OF ETL PIPELINE.")
    logger.info("==============================================")