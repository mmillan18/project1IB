from sqlalchemy import create_engine, text
from src.extract import extract
from src.load import load
from src.config import get_csv_to_table_mapping, DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL

# Crear una conexión a la base de datos SQLite en memoria
engine = create_engine("sqlite://")

# Extraer los datos de los archivos CSV y la API de días festivos
csv_table_mapping = get_csv_to_table_mapping()
csv_dataframes = extract(DATASET_ROOT_PATH, csv_table_mapping, PUBLIC_HOLIDAYS_URL)

# Cargar los datos en la base de datos SQLite
load(data_frames=csv_dataframes, database=engine)

# Verificar las tablas cargadas
with engine.connect() as connection:
    tables = connection.execute(text("SELECT name FROM sqlite_master WHERE type='table';")).fetchall()
    print("Tablas en la base de datos:")
    for table in tables:
        print(table[0])
