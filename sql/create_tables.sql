CREATE TABLE IF NOT EXISTS Dim_State (
    state_key INTEGER PRIMARY KEY AUTOINCREMENT,
    state_name TEXT NOT NULL UNIQUE,
    is_successful INTEGER NOT NULL  -- 1 para 'successful', 0 para otros
);

CREATE TABLE IF NOT EXISTS Dim_Category (
    category_key INTEGER PRIMARY KEY AUTOINCREMENT,
    main_category_name TEXT NOT NULL,
    sub_category_name TEXT NOT NULL,
    UNIQUE(main_category_name, sub_category_name)
);

-- Dimensión: Fecha (Dim_Date) - Solo para ilustración. La crearemos en el script Python
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_key INTEGER PRIMARY KEY, 
    full_date TEXT NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week TEXT NOT NULL,
    is_weekend INTEGER NOT NULL  -- 1 for weekends and 0 for weekdays
);

-- Tabla de Hechos: Campañas (Fact_Campaigns)
CREATE TABLE IF NOT EXISTS Fact_Campaigns (
    campaign_id INTEGER PRIMARY KEY, 
    name TEXT NOT NULL,
    backers INTEGER,
    pledged_usd REAL,
    goal_usd REAL,
    duration_days REAL,

    -- Claves foráneas (Foreign Keys)
    state_key INTEGER,
    category_key INTEGER,
    launched_date_key INTEGER,
    
    FOREIGN KEY (state_key) REFERENCES Dim_State (state_key),
    FOREIGN KEY (category_key) REFERENCES Dim_Category (category_key),
    FOREIGN KEY (launched_date_key) REFERENCES Dim_Date (date_key)
);