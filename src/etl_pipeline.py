import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
import sys
import sqlite3 

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

    initial_rows = len(df)
    df = df.dropna(subset=['name'])

    rows_removed = initial_rows - len(df)
    if rows_removed > 0:
        logger.warning(f"Removed {rows_removed} rows because the 'name' field was null (NOT NULL constraint).")
    else:
        logger.info("No null values found in critical NOT NULL columns.")

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

# --- LOAD PHASE FUNCTIONS ---
DB_FILE = "data/kickstarter_warehouse.db" # Nombre del archivo de base de datos SQLite

def create_db_schema(conn: sqlite3.Connection):
    """
    Create the database schema for the data warehouse.
    1. Dim_State
    """
    logger.info(f"Connected to database: {DB_FILE}. Creating schema...")
    try:
        with open("sql/create_tables.sql", 'r') as f:
            sql_script = f.read()
        conn.executescript(sql_script)
        conn.commit()
        logger.info("Dimension and fact tables schema created successfully.")
    except FileNotFoundError:
        logger.error("Error: The file 'sql/create_tables.sql' was not found.", exc_info=True)
    except Exception as e:
        logger.error(f"Error executing the SQL script: {e}", exc_info=True)

def load_dim_date(df: pd.DataFrame, conn: sqlite3.Connection) -> dict:
    """
    Genera y carga la Dimensión de Fecha (Dim_Date) a partir de las fechas de lanzamiento únicas.
    Devuelve un diccionario de mapeo: 'YYYY-MM-DD' -> date_key (YYYYMMDD).
    """
    logger.info("Initiating load of Dim_Date...")
    cursor = conn.cursor()
    
    # 1. Obtener todas las fechas únicas de lanzamiento
    unique_dates = df['launched_at'].dt.date.astype(str).unique()
    
    # 2. Create a temporary dimension DataFrame from unique dates
    date_df = pd.DataFrame({'full_date': unique_dates})
    date_df['full_date'] = pd.to_datetime(date_df['full_date'])

    date_df['full_date_str'] = date_df['full_date'].dt.strftime('%Y-%m-%d')

    # 3. Generate dimension attributes
    date_df['year'] = date_df['full_date'].dt.year
    date_df['month'] = date_df['full_date'].dt.month
    date_df['day'] = date_df['full_date'].dt.day
    date_df['quarter'] = date_df['full_date'].dt.quarter
    date_df['day_of_week'] = date_df['full_date'].dt.day_name()
    date_df['is_weekend'] = date_df['full_date'].apply(lambda x: 1 if x.weekday() >= 5 else 0) # 5=Sábado, 6=Domingo
    
    # 4. Generar la clave de fecha (date_key) como un entero YYYYMMDD
    date_df['date_key'] = date_df['full_date'].dt.strftime('%Y%m%d').astype(int)
    
    # 5. Preparar los datos para la inserción
    insert_data = date_df[[
        'date_key', 'full_date_str', 'year', 'quarter', 'month', 'day', 'day_of_week', 'is_weekend'
    ]].values.tolist()
    
    # 6. Insertar en la tabla Dim_Date
    insert_sql = """
    INSERT OR IGNORE INTO Dim_Date (date_key, full_date, year, quarter, month, day, day_of_week, is_weekend) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(insert_sql, insert_data)
    conn.commit()
    logger.info(f"Dim_Date cargada con {len(date_df)} fechas únicas.")
    
    # 7. Crear el mapeo: 'YYYY-MM-DD' -> date_key
    date_df['full_date_str'] = date_df['full_date'].dt.strftime('%Y-%m-%d')
    date_map = date_df.set_index('full_date_str')['date_key'].to_dict()
    
    return date_map

def load_data(df: pd.DataFrame, conn: sqlite3.Connection):
    """
    Decompose the DataFrame into fact and dimension tables and load them into the DB.
    """
    logger.info("Starting the Load (L) phase into the Data Warehouse.")
    cursor = conn.cursor()

    # --- 0. OBTAIN DATE MAPPING ---
    date_map = load_dim_date(df, conn)

    # --- 1. Load Dim_State ---
    logger.info("Loading Dimension Dim_State...")
    dim_state_data = df[['state', 'success_flag']].drop_duplicates().sort_values('state')
    
    state_map = {}
    for index, row in dim_state_data.iterrows():
        # Try to insert, ignoring if it already exists (to ensure uniqueness)
        cursor.execute(
            "INSERT OR IGNORE INTO Dim_State (state_name, is_successful) VALUES (?, ?)", 
            (row['state'], row['success_flag'])
        )
        # Get the generated (or existing) key for mapping
        cursor.execute("SELECT state_key FROM Dim_State WHERE state_name = ?", (row['state'],))
        state_map[row['state']] = cursor.fetchone()[0]
    
    conn.commit()
    logger.info(f"Dim_State loaded. Found {len(state_map)} unique states.")
    
    # --- 2. Load Dim_Category ---
    logger.info("Loading Dimension Dim_Category...")
    dim_category_data = df[['main_category', 'category']].drop_duplicates().sort_values(['main_category', 'category'])

    category_map = {}
    for index, row in dim_category_data.iterrows():
        cursor.execute(
            "INSERT OR IGNORE INTO Dim_Category (main_category_name, sub_category_name) VALUES (?, ?)",
            (row['main_category'], row['category'])
        )
        cursor.execute("SELECT category_key FROM Dim_Category WHERE main_category_name = ? AND sub_category_name = ?", 
                       (row['main_category'], row['category']))
        category_map[(row['main_category'], row['category'])] = cursor.fetchone()[0]

    conn.commit()
    logger.info(f"Dim_Category loaded. Found {len(category_map)} unique categories.")

    # --- 3. Prepare and Load Fact_Campaigns ---
    logger.info("Preparing and loading Fact_Campaigns...")
    
    # Map the fact data with foreign keys
    df['state_key'] = df['state'].map(state_map)
    df['category_key'] = df.apply(lambda row: category_map.get((row['main_category'], row['category'])), axis=1)
    df['launched_date_key'] = df['launched_at'].dt.strftime('%Y-%m-%d').map(date_map)

    # Select the final columns for the fact table
    fact_columns = [
        'ID', 'name', 'backers', 'pledged_usd', 'goal_usd', 'duration_days', 
        'state_key', 'category_key', 'launched_date_key'
    ]
    fact_data = df[fact_columns]

    # Insertar en la Tabla de Hechos (Fact_Campaigns)
    insert_sql = """
    INSERT INTO Fact_Campaigns (campaign_id, name, backers, pledged_usd, goal_usd, 
                                duration_days, state_key, category_key, launched_date_key) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(insert_sql, fact_data.values.tolist())
    conn.commit()
    logger.info(f"Fact_Campaigns cargada con {len(fact_data)} registros.")

    cursor.close()
    conn.close()


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

        # ---------------- LOAD PHASE ----------------
        try:
            conn = sqlite3.connect(DB_FILE)
            create_db_schema(conn)
            load_data(transformed_df, conn)
        except Exception as e:
            logger.critical(f"CRITICAL FAILURE - LOADING DATA: {e}", exc_info=True)
        finally:
            if 'conn' in locals() and conn:
                conn.close()
                logger.info("Connection to the database closed.")
    
    logger.info("END OF ETL PIPELINE.")
    logger.info("==============================================")