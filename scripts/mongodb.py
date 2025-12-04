from pathlib import Path
import pandas as pd
from pymongo import MongoClient
from datetime import datetime


def connect(connection_string):
    """
    Conecta a MongoDB Atlas usando la cadena de conexión
    """
    try:
        client = MongoClient(connection_string)
        # Verificar conexión
        client.admin.command("ping")
        print("✓ Conexión exitosa a MongoDB Atlas")
        return client
    except Exception as e:
        print(f"✗ Error de conexión: {e}")
        return None


def import_csv(csv_path, db_name, collection_name, client):
    """
    Lee un CSV y lo inserta en una colección de MongoDB
    """
    try:
        # Leer CSV con opciones robustas para manejar errores de formato
        df = pd.read_csv(
            csv_path,
            engine="python",  # Motor más flexible
        )
        print(f"✓ CSV leído: {len(df)} registros encontrados")

        # Convertir fechas a formato datetime
        if "FullDate" in df.columns:
            df["FullDate"] = pd.to_datetime(df["FullDate"])

        # Convertir DataFrame a diccionarios
        datos = df.to_dict("records")

        # Acceder a la base de datos y colección
        db = client[db_name]

        # Eliminar la colección completamente si existe (sobreescribir)
        if collection_name in db.list_collection_names():
            db.drop_collection(collection_name)
            print(f"✓ Colección '{collection_name}' eliminada (sobreescritura)")

        # Crear nueva colección
        collection = db[collection_name]

        # Insertar datos
        resultado = collection.insert_many(datos)
        print(
            f"✓ {len(resultado.inserted_ids)} documentos insertados en nueva colección"
        )

        return collection
    except Exception as e:
        print(f"✗ Error al importar CSV: {e}")
        return None


def monthly_trend(collection):
    """
    Analiza la tendencia de ventas mensuales por categoría de producto
    para identificar patrones estacionales
    """
    print("\n" + "=" * 60)
    print("CONSULTA 1: Tendencia de Ventas Mensuales por Categoría")
    print("=" * 60)

    pipeline = [
        {
            "$group": {
                "_id": {
                    "year": "$Year",
                    "month": "$MonthNumber",
                    "categoria": "$ProductCategory",
                },
                "ventas_totales": {"$sum": "$SalesAmount"},
                "cantidad_vendida": {"$sum": "$SalesQuantity"},
                "rentabilidad_promedio": {"$avg": "$TotalCost"},
            }
        },
        {"$sort": {"_id.year": 1, "_id.month": 1}},
        {
            "$project": {
                "_id": 0,
                "año": "$_id.year",
                "mes": "$_id.month",
                "categoria": "$_id.categoria",
                "ventas_totales": {"$round": ["$ventas_totales", 2]},
                "cantidad_vendida": 1,
                "costo_promedio": {"$round": ["$rentabilidad_promedio", 2]},
            }
        },
    ]

    resultados = list(collection.aggregate(pipeline))

    # Mostrar primeros 10 resultados
    for i, doc in enumerate(resultados[:10], 1):
        print(f"\n{i}. {doc['categoria']} - {doc['mes']}/{doc['año']}")
        print(f"   Ventas: ${doc['ventas_totales']:,.2f}")
        print(f"   Unidades: {doc['cantidad_vendida']}")
        print(f"   Costo promedio: ${doc['costo_promedio']:.2f}")

    print(f"\n... Total de registros: {len(resultados)}")
    return resultados


def best_products(collection):
    """
    Identifica los productos con mejor rendimiento por canal de venta
    """
    print("\n" + "=" * 60)
    print("CONSULTA 2: Top Productos por Canal de Venta")
    print("=" * 60)

    pipeline = [
        {
            "$group": {
                "_id": {
                    "producto": "$ProductName",
                    "canal": "$ChannelName",
                    "marca": "$BrandName",
                },
                "ventas_totales": {"$sum": "$SalesAmount"},
                "unidades_vendidas": {"$sum": "$SalesQuantity"},
                "descuento_promedio": {"$avg": "$DiscountPercent"},
                "rentabilidad": {"$sum": {"$subtract": ["$SalesAmount", "$TotalCost"]}},
            }
        },
        {"$sort": {"ventas_totales": -1}},
        {"$limit": 15},
        {
            "$project": {
                "_id": 0,
                "producto": "$_id.producto",
                "canal": "$_id.canal",
                "marca": "$_id.marca",
                "ventas_totales": {"$round": ["$ventas_totales", 2]},
                "unidades_vendidas": 1,
                "descuento_promedio": {"$round": ["$descuento_promedio", 4]},
                "rentabilidad": {"$round": ["$rentabilidad", 2]},
            }
        },
    ]

    resultados = list(collection.aggregate(pipeline))

    for i, doc in enumerate(resultados, 1):
        print(f"\n{i}. {doc['producto']}")
        print(f"   Canal: {doc['canal']} | Marca: {doc['marca']}")
        print(f"   Ventas: ${doc['ventas_totales']:,.2f}")
        print(f"   Unidades: {doc['unidades_vendidas']}")
        print(f"   Rentabilidad: ${doc['rentabilidad']:,.2f}")
        print(f"   Descuento promedio: {doc['descuento_promedio'] * 100:.2f}%")

    return resultados


def geographical_analysis(collection):
    """
    Analiza el rendimiento de ventas por región geográfica y estacionalidad
    """
    print("\n" + "=" * 60)
    print("CONSULTA 3: Análisis Geográfico y Estacional")
    print("=" * 60)

    pipeline = [
        {
            "$group": {
                "_id": {
                    "region": "$RegionCountryName",
                    "trimestre": {"$ceil": {"$divide": ["$MonthNumber", 3]}},
                    "año": "$Year",
                },
                "ventas_totales": {"$sum": "$SalesAmount"},
                "tiendas_activas": {"$addToSet": "$StoreName"},
                "productos_vendidos": {"$sum": "$SalesQuantity"},
                "devoluciones": {"$sum": "$ReturnAmount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "region": "$_id.region",
                "trimestre": "$_id.trimestre",
                "año": "$_id.año",
                "ventas_totales": {"$round": ["$ventas_totales", 2]},
                "num_tiendas": {"$size": "$tiendas_activas"},
                "productos_vendidos": 1,
                "devoluciones": {"$round": ["$devoluciones", 2]},
                "tasa_devolucion": {
                    "$round": [
                        {
                            "$multiply": [
                                {"$divide": ["$devoluciones", "$ventas_totales"]},
                                100,
                            ]
                        },
                        2,
                    ]
                },
            }
        },
        {"$sort": {"ventas_totales": -1}},
        {"$limit": 20},
    ]

    resultados = list(collection.aggregate(pipeline))

    for i, doc in enumerate(resultados, 1):
        print(f"\n{i}. {doc['region']} - Q{doc['trimestre']} {doc['año']}")
        print(f"   Ventas: ${doc['ventas_totales']:,.2f}")
        print(f"   Tiendas activas: {doc['num_tiendas']}")
        print(f"   Productos vendidos: {doc['productos_vendidos']}")
        print(
            f"   Devoluciones: ${doc['devoluciones']:,.2f} ({doc['tasa_devolucion']}%)"
        )

    return resultados


def main():
    # Configurar conexión de mongodb.
    CONNECTION_STRING = (
        "mongodb+srv://user:password@cluster0.qomntw2.mongodb.net/?appName=Cluster0"
    )

    # Configuraciones.
    CSV_PATH = f"{Path(__file__).parent}/../modelo_ventas.csv"
    DB_NAME = "modelo_ventas_db"
    COLLECTION_NAME = "modelo_ventas"

    # Conectar a MongoDB
    client = connect(CONNECTION_STRING)
    if not client:
        print("No se pudo establecer conexión con la base de datos")
        return

    # Importar CSV
    collection = import_csv(CSV_PATH, DB_NAME, COLLECTION_NAME, client)
    if collection is None:
        return

    # Ejecutar consultas
    monthly_trend(collection)
    best_products(collection)
    geographical_analysis(collection)

    print("\n" + "=" * 60)
    print("Análisis completado exitosamente")
    print("=" * 60)

    # Cerrar conexión
    client.close()


if __name__ == "__main__":
    main()
