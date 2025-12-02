import pymssql
from pathlib import Path

from scripts.data_proceessing import process_data
from scripts.db import create_stagging_db, SQL_SERVER_CONFIG as sqc
from scripts.db2 import create_dwh_db, SQL_SERVER_CONFIG as sqc2
from scripts.load_data import main, excute_migration

DATA_DIR = Path(__file__).parent / "market_db"


def run():
    print("Iniciando procesamiento de datos...")
    process_data()
    print("Procesamiento de datos completado.")

    print("\n------------------------------------------\n")

    print(f"Creando Base de Datos: {sqc['database']}")
    create_stagging_db()
    print(f"Base de Datos {sqc['database']} creada.")

    print("\n------------------------------------------\n")

    print(f"Creando Base de Datos: {sqc2['database']}")
    create_dwh_db()
    print(f"Base de Datos {sqc2['database']} creada.")

    print("\n------------------------------------------\n")

    print("Iniciando carga de datos a la Base de Datos Stagging...")
    main(DATA_DIR=DATA_DIR)
    print("Carga de datos completada.")

    print("\n------------------------------------------\n")

    print("Iniciando migración de datos a la Base de Datos DWH...")
    conn = pymssql.connect(
        server=sqc["server"],
        port=sqc["port"],
        user=sqc["user"],
        password=sqc["password"],
        database=sqc["database"],
        autocommit=True,
    )
    cursor = conn.cursor()
    excute_migration(cursor)
    print("Migración de datos completada.")


if __name__ == "__main__":
    run()

