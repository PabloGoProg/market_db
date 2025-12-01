import pymssql
from pathlib import Path

# Configuración SQL Server
SQL_SERVER_CONFIG = {
    "server": "172.31.32.1",
    "port": 1433,
    "user": "test_conn",
    "password": "@Pg621327481",
    "database": "BI_Ventas_Stagging",
    "schema": "SQLBI",
}

DATA_DIR = Path(__file__).parent / "data"

def create_stagging_db():
    conn = pymssql.connect(
        server=SQL_SERVER_CONFIG["server"],
        port=SQL_SERVER_CONFIG["port"],
        user=SQL_SERVER_CONFIG["user"],
        password=SQL_SERVER_CONFIG["password"],
        autocommit=True,
    )
    cursor = conn.cursor()

    cursor.execute(
        f"""
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{SQL_SERVER_CONFIG["database"]}')
            CREATE DATABASE {SQL_SERVER_CONFIG["database"]}
    """
    )

    # Reconectar a la base de datos específica
    conn.close()
    conn = pymssql.connect(
        server=SQL_SERVER_CONFIG["server"],
        port=SQL_SERVER_CONFIG["port"],
        user=SQL_SERVER_CONFIG["user"],
        password=SQL_SERVER_CONFIG["password"],
        database=SQL_SERVER_CONFIG["database"],
        autocommit=True,
    )
    cursor = conn.cursor()

    # crear esquema si no existe
    cursor.execute(
        f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{SQL_SERVER_CONFIG["schema"]}')
            EXEC('CREATE SCHEMA {SQL_SERVER_CONFIG["schema"]}')
    """
    )

    # Eliminar tablas
    tables = [
        "Sales",
        "Channel",
        "Geography"
        "Stores",
        "Product",
        "ProductSubcategory",
        "ProductCategory",
        "Promotion",
    ]
    for table in tables:
        cursor.execute(
            f"IF OBJECT_ID('{SQL_SERVER_CONFIG["schema"]}.{table}', 'U') IS NOT NULL DROP TABLE {SQL_SERVER_CONFIG["schema"]}.{table}"
        )

    # Channel
    cursor.execute(
        f"""
        CREATE TABLE {SQL_SERVER_CONFIG["schema"]}.Channel (
            ChannelID INT PRIMARY KEY,
            ChannelName NVARCHAR(50) NOT NULL
        )
    """
    )

    # Geography
    cursor.execute(
        f"""
        CREATE TABLE {SQL_SERVER_CONFIG["schema"]}.Geography (
            GeographyKey INT PRIMARY KEY,
            ContinentName NVARCHAR(50),
            GeographyType NVARCHAR(50),
            RegionCountryName NVARCHAR(100)
        )
    """
    )

    # ProductCategory
    cursor.execute(
        f"""
        CREATE TABLE {SQL_SERVER_CONFIG["schema"]}.ProductCategory (
            ProductCategoryKey INT PRIMARY KEY,
            ProductCategory NVARCHAR(100) NOT NULL
        )
    """
    )

    # ProductSubcategory
    cursor.execute(
        f"""
        CREATE TABLE {SQL_SERVER_CONFIG["schema"]}.ProductSubcategory (
            ProductSubcategoryKey INT PRIMARY KEY,
            ProductSubcategory NVARCHAR(100) NOT NULL,
            ProductCategoryKey INT NOT NULL,
            CONSTRAINT FK_ProductSubcategory_ProductCategory 
                FOREIGN KEY (ProductCategoryKey) 
                REFERENCES {SQL_SERVER_CONFIG["schema"]}.ProductCategory(ProductCategoryKey)
        )
    """
    )

    # Product
    cursor.execute(
        f"""
        CREATE TABLE {SQL_SERVER_CONFIG["schema"]}.Product (
            ProductKey INT PRIMARY KEY,
            ProductName NVARCHAR(200) NOT NULL,
            BrandName NVARCHAR(100),
            ClassName NVARCHAR(50),
            ColorName NVARCHAR(50),
            Manufacturer NVARCHAR(100),
            ProductDescription NVARCHAR(MAX),
            ProductSubcategoryKey INT,
            UnitCost DECIMAL(18, 2),
            UnitPrice DECIMAL(18, 2),
            CONSTRAINT FK_Product_ProductSubcategory 
                FOREIGN KEY (ProductSubcategoryKey) 
                REFERENCES {SQL_SERVER_CONFIG["schema"]}.ProductSubcategory(ProductSubcategoryKey)
        )
    """
    )

    # Promotion
    cursor.execute(
        f"""
        CREATE TABLE {SQL_SERVER_CONFIG["schema"]}.Promotion (
            PromotionKey INT PRIMARY KEY,
            PromotionName NVARCHAR(100),
            PromotionLabel NVARCHAR(100),
            PromotionDescription NVARCHAR(MAX),
            DiscountPercent DECIMAL(5, 2),
            PromotionCategory NVARCHAR(50),
            PromotionType NVARCHAR(50),
            StartDate DATE,
            EndDate DATE,
            MinQuantity INT,
            MaxQuantity INT,
            Priority INT
        )
    """
    )

    # Stores
    cursor.execute(
        f"""
        CREATE TABLE {SQL_SERVER_CONFIG["schema"]}.Stores (
            StoreKey INT PRIMARY KEY,
            GeographyKey INT,
            StoreName NVARCHAR(100) NOT NULL,
            StoreType NVARCHAR(50),
            Status NVARCHAR(20),
            SellingAreaSize INT,
            EmployeeCount INT,
            CloseReason NVARCHAR(100),
            CONSTRAINT FK_Stores_Geography 
                FOREIGN KEY (GeographyKey) 
                REFERENCES {SQL_SERVER_CONFIG["schema"]}.Geography(GeographyKey)
        )
    """
    )

    # TABLA DE HECHOS: Sales
    cursor.execute(
        f"""
        CREATE TABLE {SQL_SERVER_CONFIG["schema"]}.Sales (
            SalesKey BIGINT PRIMARY KEY,
            DateKey DATETIME,
            channel INT,
            StoreKey INT,
            Product_ID INT,
            PromotionKey INT,
            SalesQuantity INT,
            SalesAmount DECIMAL(18, 2),
            ReturnQuantity INT,
            ReturnAmount DECIMAL(18, 2),
            DiscountQuantity INT,
            DiscountAmount DECIMAL(18, 2),
            TotalCost DECIMAL(18, 2),
            UnitCost DECIMAL(18, 2),
            UnitPrice DECIMAL(18, 2),
            CONSTRAINT FK_Sales_Channel 
                FOREIGN KEY (channel) 
                REFERENCES {SQL_SERVER_CONFIG["schema"]}.Channel(ChannelID),
            CONSTRAINT FK_Sales_Stores 
                FOREIGN KEY (StoreKey) 
                REFERENCES {SQL_SERVER_CONFIG["schema"]}.Stores(StoreKey),
            CONSTRAINT FK_Sales_Product 
                FOREIGN KEY (Product_ID) 
                REFERENCES {SQL_SERVER_CONFIG["schema"]}.Product(ProductKey),
            CONSTRAINT FK_Sales_Promotion 
                FOREIGN KEY (PromotionKey) 
                REFERENCES {SQL_SERVER_CONFIG["schema"]}.Promotion(PromotionKey)
        )
    """
    )


    indexes = [
        ("IX_Sales_DateKey", f"{SQL_SERVER_CONFIG["schema"]}.Sales", "DateKey"),
        ("IX_Sales_Channel", f"{SQL_SERVER_CONFIG["schema"]}.Sales", "channel"),
        ("IX_Sales_StoreKey", f"{SQL_SERVER_CONFIG["schema"]}.Sales", "StoreKey"),
        ("IX_Sales_ProductID", f"{SQL_SERVER_CONFIG["schema"]}.Sales", "Product_ID"),
        ("IX_Sales_PromotionKey", f"{SQL_SERVER_CONFIG["schema"]}.Sales", "PromotionKey"),
        (
            "IX_Product_SubcategoryKey",
            f"{SQL_SERVER_CONFIG["schema"]}.Product",
            "ProductSubcategoryKey",
        ),
        (
            "IX_ProductSubcategory_CategoryKey",
            f"{SQL_SERVER_CONFIG["schema"]}.ProductSubcategory",
            "ProductCategoryKey",
        ),
        ("IX_Stores_GeographyKey", f"{SQL_SERVER_CONFIG["schema"]}.Stores", "GeographyKey"),
    ]

    for index_name, table_name, column_name in indexes:
        cursor.execute(
            f"""
            CREATE NONCLUSTERED INDEX {index_name}
            ON {table_name} ({column_name})
        """
        )
        print(f"   ✓ Índice {index_name} creado")


    # Listar tablas
    cursor.execute(
        """
        SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS FullTableName
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'SQLBI'
        ORDER BY TABLE_NAME
    """
    )
    tables = cursor.fetchall()
    print(f"\n   Tablas creadas ({len(tables)}):")
    for table in tables:
        print(f"      - {table[0]}")

    # Listar Foreign Keys
    cursor.execute(
        """
        SELECT 
            fk.name AS FK_Name,
            OBJECT_SCHEMA_NAME(fk.parent_object_id) + '.' + OBJECT_NAME(fk.parent_object_id) AS Table_Name,
            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS Column_Name,
            OBJECT_SCHEMA_NAME(fk.referenced_object_id) + '.' + OBJECT_NAME(fk.referenced_object_id) AS Referenced_Table,
            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS Referenced_Column
        FROM sys.foreign_keys AS fk
        INNER JOIN sys.foreign_key_columns AS fc 
            ON fk.object_id = fc.constraint_object_id
        WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = 'SQLBI'
        ORDER BY Table_Name
    """
    )
    fks = cursor.fetchall()
    print(f"\n   Foreign Keys creadas ({len(fks)}):")
    for fk in fks:
        print(f"      - {fk[1]}.{fk[2]} → {fk[3]}.{fk[4]} ({fk[0]})")

    # Cerrar conexión
    conn.close()
