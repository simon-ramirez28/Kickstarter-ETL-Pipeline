import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
import sys

# --- LOGGER CONFIGURATION ---
LOG_FILE = "logs/etl_pipeline.log"
# Point the logger to the log directory, create if it doesn't exist
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

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make the necessary transformations to clean and model the data.
    """
    logger.info("Starting Data Transformation phase.")
    
    # --- 1. Cleaning and Creating Dates/Times ---
    # Convert date columns to datetime format
    df['launched_at'] = pd.to_datetime(df['launched'])
    df['deadline_at'] = pd.to_datetime(df['deadline'])
    
    # Calculate campaign duration in days
    df['duration_days'] = (df['deadline_at'] - df['launched_at']).dt.total_seconds() / (60 * 60 * 24)
    logger.info("Dates converted to datetime and campaign duration calculated.")
    
    # --- 2. Currency Unification and Key Metrics ---
    # Rename 'real' columns for clarity, ensuring we work in USD.
    df = df.rename(columns={
        'usd_pledged_real': 'pledged_usd',
        'usd_goal_real': 'goal_usd'
    })
    logger.info("Monetary columns renamed to 'pledged_usd' and 'goal_usd'.")
    
    # --- 3. Success Flag Creation ---
    # Map the state to 1 (success) or 0 (failure, canceled, suspended, etc.)
    SUCCESS_STATES = ['successful']
    df['success_flag'] = df['state'].apply(lambda x: 1 if x in SUCCESS_STATES else 0)
    logger.info("Binary flag 'success_flag' created.")

    # --- 4. Final Column Selection and Ordering ---
    # Create a final DataFrame with only the columns we will use for modeling
    final_columns = [
        'ID', 
        'name', 
        'main_category', 
        'category',
        'country', 
        'backers', 
        'pledged_usd', 
        'goal_usd',
        'success_flag', 
        'state', # Keep for State Dimension
        'launched_at', 
        'deadline_at', 
        'duration_days'
    ]
    df_transformed = df[final_columns].copy()
    
    logger.info(f"Transformation completed. New DataFrame has {df_transformed.shape[0]} rows and {df_transformed.shape[1]} columns.")
    return df_transformed


if __name__ == "__main__":
    logger.info("==============================================")
    logger.info("START OF KICKSTARTER ETL PIPELINE")
    
    kickstarter_df = extract_data(FILE_PATH)
    
    if kickstarter_df is not None:
        inspect_data(kickstarter_df)

        # Call the transformation function
        transformed_df = transform_data(kickstarter_df)

        # Quick inspection of the transformed data
        logger.info("Inspecting transformed data:")
        logger.info(f"Unique values in 'success_flag':\n{transformed_df['success_flag'].value_counts().to_string()}")
        logger.info(f"Null values in 'pledged_usd': {transformed_df['pledged_usd'].isnull().sum()}")
    
    logger.info("END OF ETL PIPELINE.")
    logger.info("==============================================")