import pymssql
import csv
from pathlib import Path
from datetime import datetime
import sys

# Configuración SQL Server
SQL_SERVER_CONFIG = {
    "server": "172.31.32.1",
    "port": 1433,
    "user": "test_conn",
    "password": "@Pg621327481",
    "database": "BI_Ventas_Stagging",
    "schema": "SQLBI",
}


def create_processes(cursor):
    cursor.execute(
        """
        CREATE PROCEDURE SQLBI.sp_InsertChannel
            @ChannelID INT,
            @ChannelName NVARCHAR(50)
        AS
        BEGIN
            SET NOCOUNT ON;
            
            IF NOT EXISTS (SELECT 1 FROM SQLBI.Channel WHERE ChannelID = @ChannelID)
            BEGIN
                INSERT INTO SQLBI.Channel (ChannelID, ChannelName)
                VALUES (@ChannelID, @ChannelName);
            END
        END
        """
    )

    cursor.execute("""
        CREATE PROCEDURE SQLBI.sp_InsertGeography
            @GeographyKey INT,
            @ContinentName NVARCHAR(50),
            @GeographyType NVARCHAR(50),
            @RegionCountryName NVARCHAR(100)
        AS
        BEGIN
            SET NOCOUNT ON;
            
            IF NOT EXISTS (SELECT 1 FROM SQLBI.Geography WHERE GeographyKey = @GeographyKey)
            BEGIN
                INSERT INTO SQLBI.Geography (GeographyKey, ContinentName, GeographyType, RegionCountryName)
                VALUES (@GeographyKey, @ContinentName, @GeographyType, @RegionCountryName);
            END
        END
    """)

    cursor.execute("""
        CREATE PROCEDURE SQLBI.sp_InsertProductCategory
            @ProductCategoryKey INT,
            @ProductCategory NVARCHAR(100)
        AS
        BEGIN
            SET NOCOUNT ON;
            
            IF NOT EXISTS (SELECT 1 FROM SQLBI.ProductCategory WHERE ProductCategoryKey = @ProductCategoryKey)
            BEGIN
                INSERT INTO SQLBI.ProductCategory (ProductCategoryKey, ProductCategory)
                VALUES (@ProductCategoryKey, @ProductCategory);
            END
        END
    """)

    cursor.execute("""
        CREATE PROCEDURE SQLBI.sp_InsertProductSubcategory
            @ProductSubcategoryKey INT,
            @ProductSubcategory NVARCHAR(100),
            @ProductCategoryKey INT
        AS
        BEGIN
            SET NOCOUNT ON;
            
            IF NOT EXISTS (SELECT 1 FROM SQLBI.ProductSubcategory WHERE ProductSubcategoryKey = @ProductSubcategoryKey)
            BEGIN
                INSERT INTO SQLBI.ProductSubcategory (ProductSubcategoryKey, ProductSubcategory, ProductCategoryKey)
                VALUES (@ProductSubcategoryKey, @ProductSubcategory, @ProductCategoryKey);
            END
        END
    """)

    cursor.execute("""
        CREATE PROCEDURE SQLBI.sp_InsertProduct
            @ProductKey INT,
            @ProductName NVARCHAR(200),
            @BrandName NVARCHAR(100),
            @ClassName NVARCHAR(50),
            @Manufacturer NVARCHAR(100),
            @ProductDescription NVARCHAR(MAX),
            @ProductSubcategoryKey INT,
            @UnitCost DECIMAL(18, 2),
            @UnitPrice DECIMAL(18, 2)
        AS
        BEGIN
            SET NOCOUNT ON;
            
            IF NOT EXISTS (SELECT 1 FROM SQLBI.Product WHERE ProductKey = @ProductKey)
            BEGIN
                INSERT INTO SQLBI.Product (
                    ProductKey, ProductName, BrandName, ClassName, 
                    Manufacturer, ProductDescription, ProductSubcategoryKey, 
                    UnitCost, UnitPrice
                )
                VALUES (
                    @ProductKey, @ProductName, @BrandName, @ClassName,
                    @Manufacturer, @ProductDescription, @ProductSubcategoryKey,
                    @UnitCost, @UnitPrice
                );
            END
        END
    """)

    cursor.execute("""
        CREATE PROCEDURE SQLBI.sp_InsertPromotion
            @PromotionKey INT,
            @PromotionName NVARCHAR(100),
            @PromotionLabel NVARCHAR(100),
            @DiscountPercent DECIMAL(5, 2),
            @StartDate DATE,
            @EndDate DATE,
            @Priority INT
        AS
        BEGIN
            SET NOCOUNT ON;
            
            IF NOT EXISTS (SELECT 1 FROM SQLBI.Promotion WHERE PromotionKey = @PromotionKey)
            BEGIN
                INSERT INTO SQLBI.Promotion (
                    PromotionKey, PromotionName, PromotionLabel, 
                    DiscountPercent, StartDate, EndDate, Priority
                )
                VALUES (
                    @PromotionKey, @PromotionName, @PromotionLabel,
                    @DiscountPercent, @StartDate, @EndDate, @Priority
                );
            END
        END               
    """)

    cursor.execute("""
        CREATE PROCEDURE SQLBI.sp_InsertStores
            @StoreKey INT,
            @GeographyKey INT,
            @StoreName NVARCHAR(100),
            @StoreType NVARCHAR(50),
            @Status NVARCHAR(20),
            @SellingAreaSize INT,
            @EmployeeCount INT,
            @CloseReason NVARCHAR(100)
        AS
        BEGIN
            SET NOCOUNT ON;
            
            IF NOT EXISTS (SELECT 1 FROM SQLBI.Stores WHERE StoreKey = @StoreKey)
            BEGIN
                INSERT INTO SQLBI.Stores (
                    StoreKey, GeographyKey, StoreName, StoreType, 
                    Status, SellingAreaSize, EmployeeCount, CloseReason
                )
                VALUES (
                    @StoreKey, @GeographyKey, @StoreName, @StoreType,
                    @Status, @SellingAreaSize, @EmployeeCount, @CloseReason
                );
            END
        END               
    """)

    cursor.execute("""
        CREATE PROCEDURE SQLBI.sp_InsertSales
            @SalesKey BIGINT,
            @DateKey DATETIME,
            @channel INT,
            @StoreKey INT,
            @Product_ID INT,
            @PromotionKey INT,
            @SalesQuantity INT,
            @SalesAmount DECIMAL(18, 2),
            @ReturnQuantity INT,
            @ReturnAmount DECIMAL(18, 2),
            @DiscountQuantity INT,
            @DiscountAmount DECIMAL(18, 2),
            @TotalCost DECIMAL(18, 2),
            @UnitCost DECIMAL(18, 2),
            @UnitPrice DECIMAL(18, 2)
        AS
        BEGIN
            SET NOCOUNT ON;
            
            IF NOT EXISTS (SELECT 1 FROM SQLBI.Sales WHERE SalesKey = @SalesKey)
            BEGIN
                INSERT INTO SQLBI.Sales (
                    SalesKey, DateKey, channel, StoreKey, Product_ID, PromotionKey,
                    SalesQuantity, SalesAmount, ReturnQuantity, ReturnAmount,
                    DiscountQuantity, DiscountAmount, TotalCost, UnitCost, UnitPrice
                )
                VALUES (
                    @SalesKey, @DateKey, @channel, @StoreKey, @Product_ID, @PromotionKey,
                    @SalesQuantity, @SalesAmount, @ReturnQuantity, @ReturnAmount,
                    @DiscountQuantity, @DiscountAmount, @TotalCost, @UnitCost, @UnitPrice
                );
            END
        END
    """)
    
def create_migration_process(cursor):
    cursor.execute(
        """
        -- Migración Base de datos

        -- Procedimiento de transición para tabla Channel
        CREATE PROCEDURE SQLBI.MigrateDB
        AS
        BEGIN
            SET NOCOUNT ON;

            INSERT INTO BI_Ventas_DWH.SQLBI.Calendar (
                DateKey,
                FullDate,
                DayOfMonth,
                DayOfWeek,
                DayOfYear,
                MonthNumber,
                MonthName,
                Year,
                IsHoliday
            )
            SELECT
                DateKey       = YEAR(S.DateKey) * 10000
                                + MONTH(S.DateKey) * 100
                                + DAY(S.DateKey),
                FullDate         = S.DateKey,
                DayOfMonth       = DAY(S.DateKey),
                DayOfWeek        = DATEPART(WEEKDAY, S.DateKey),
                DayOfYear        = DATEPART(DAYOFYEAR, S.DateKey),
                MonthNumber      = MONTH(S.DateKey),
                MonthName        = DATENAME(MONTH, S.DateKey),
                [Year]           = YEAR(S.DateKey),
                IsHoliday        = 0
            FROM BI_Ventas_Stagging.SQLBI.Sales AS S
            LEFT JOIN BI_Ventas_DWH.SQLBI.Calendar AS C
                ON C.FullDate = S.DateKey
            WHERE
                S.DateKey IS NOT NULL
            GROUP BY
                S.DateKey,
                YEAR(S.DateKey),
                MONTH(S.DateKey),
                DAY(S.DateKey),
                DATEPART(WEEKDAY, S.DateKey),
                DATEPART(DAYOFYEAR, S.DateKey),
                DATENAME(MONTH, S.DateKey)
            HAVING
                MAX(C.DateKey) IS NULL;

            -- Migración de Tabla Channel
            MERGE INTO BI_Ventas_DWH.SQLBI.Channel AS T
            USING BI_Ventas_Stagging.SQLBI.Channel AS S
                ON T.ChannelID = S.ChannelID
            WHEN MATCHED THEN
                UPDATE SET
                    T.ChannelName = S.ChannelName
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (ChannelID, ChannelName)
                VALUES (S.ChannelID, S.ChannelName);

            -- Migración de Tabla Geography
            MERGE INTO BI_Ventas_DWH.SQLBI.Geography AS T
            USING BI_Ventas_Stagging.SQLBI.Geography AS S
                ON T.GeographyKey = S.GeographyKey
            WHEN MATCHED THEN
                UPDATE SET
                    T.ContinentName = S.ContinentName,
                    T.GeographyType = S.GeographyType,
                    T.RegionCountryName = S.RegionCountryName
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (GeographyKey, ContinentName, GeographyType, RegionCountryName)
                VALUES (S.GeographyKey, S.ContinentName, S.GeographyType, S.RegionCountryName);

            -- Migración de Tabla ProductCategory
            MERGE INTO BI_Ventas_DWH.SQLBI.ProductCategory AS T
            USING BI_Ventas_Stagging.SQLBI.ProductCategory AS S
                ON T.ProductCategoryKey = S.ProductCategoryKey
            WHEN MATCHED THEN
                UPDATE SET
                    T.ProductCategory = S.ProductCategory
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (ProductCategoryKey, ProductCategory)
                VALUES (S.ProductCategoryKey, S.ProductCategory);

            -- Migración de Tabla ProductSubcategory 
            MERGE INTO BI_Ventas_DWH.SQLBI.ProductSubcategory AS T
            USING BI_Ventas_Stagging.SQLBI.ProductSubcategory AS S
                ON T.ProductSubcategoryKey = S.ProductSubcategoryKey
            WHEN MATCHED THEN
                UPDATE SET
                    T.ProductSubcategory = S.ProductSubcategory,
                    T.ProductCategoryKey = S.ProductCategoryKey
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (ProductSubcategoryKey, ProductSubcategory, ProductCategoryKey)
                VALUES (S.ProductSubcategoryKey, S.ProductSubcategory, S.ProductCategoryKey);

            -- Migración de Tabla ProductSubcategory Product
            MERGE INTO BI_Ventas_DWH.SQLBI.Product AS T
            USING BI_Ventas_Stagging.SQLBI.Product AS S
                ON T.ProductKey = S.ProductKey
            WHEN MATCHED THEN
                UPDATE SET
                    T.BrandName            = S.BrandName,
                    T.ClassName            = S.ClassName,
                    T.Manufacturer         = S.Manufacturer,
                    T.ProductDescription   = S.ProductDescription,
                    T.ProductName          = S.ProductName,
                    T.ProductSubcategoryKey= S.ProductSubcategoryKey,
                    T.UnitCost             = S.UnitCost,
                    T.UnitPrice            = S.UnitPrice
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    ProductKey,
                    BrandName,
                    ClassName,
                    Manufacturer,
                    ProductDescription,
                    ProductName,
                    ProductSubcategoryKey,
                    UnitCost,
                    UnitPrice
                )
                VALUES (
                    S.ProductKey,
                    S.BrandName,
                    S.ClassName,
                    S.Manufacturer,
                    S.ProductDescription,
                    S.ProductName,
                    S.ProductSubcategoryKey,
                    S.UnitCost,
                    S.UnitPrice
                );

            -- Migración de Tabla Promotion
            MERGE INTO BI_Ventas_DWH.SQLBI.Promotion AS T
            USING BI_Ventas_Stagging.SQLBI.Promotion AS S
                ON T.PromotionKey = S.PromotionKey
            WHEN MATCHED THEN
                UPDATE SET
                    T.PromotionName   = S.PromotionName,
                    T.PromotionLabel  = S.PromotionLabel,
                    T.DiscountPercent = S.DiscountPercent,
                    T.StartDate       = S.StartDate,
                    T.EndDate         = S.EndDate,
                    T.Priority        = S.Priority
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    PromotionKey,
                    PromotionName,
                    PromotionLabel,
                    DiscountPercent,
                    StartDate,
                    EndDate,
                    Priority
                )
                VALUES (
                    S.PromotionKey,
                    S.PromotionName,
                    S.PromotionLabel,
                    S.DiscountPercent,
                    S.StartDate,
                    S.EndDate,
                    S.Priority
                );

            -- Migración de Tabla Store
            MERGE INTO BI_Ventas_DWH.SQLBI.Stores AS T
            USING BI_Ventas_Stagging.SQLBI.Stores AS S
                ON T.StoreKey = S.StoreKey
            WHEN MATCHED THEN
                UPDATE SET
                    T.StoreName       = S.StoreName,
                    T.StoreType       = S.StoreType,
                    T.Status          = S.Status,
                    T.CloseReason     = S.CloseReason,
                    T.EmployeeCount   = S.EmployeeCount,
                    T.GeographyKey    = S.GeographyKey,
                    T.SellingAreaSize = S.SellingAreaSize
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    StoreKey,
                    StoreName,
                    StoreType,
                    Status,
                    CloseReason,
                    EmployeeCount,
                    GeographyKey,
                    SellingAreaSize
                )
                VALUES (
                    S.StoreKey,
                    S.StoreName,
                    S.StoreType,
                    S.Status,
                    S.CloseReason,
                    S.EmployeeCount,
                    S.GeographyKey,
                    S.SellingAreaSize
                );

            -- Migración de Tabla Sale
            MERGE INTO BI_Ventas_DWH.SQLBI.Sales AS T
            USING (
                SELECT
                    S.SalesKey,
                    S.channel,
                    C.DateKey AS DateKey,
                    S.DiscountAmount,
                    S.DiscountQuantity,
                    S.Product_ID,
                    S.PromotionKey,
                    S.ReturnAmount,
                    S.ReturnQuantity,
                    S.SalesAmount,
                    S.SalesQuantity,
                    S.StoreKey,
                    S.TotalCost,
                    S.UnitCost,
                    S.UnitPrice
                FROM BI_Ventas_Stagging.SQLBI.Sales AS S
                INNER JOIN BI_Ventas_DWH.SQLBI.Calendar AS C
                    ON C.FullDate = S.DateKey
            ) AS S
            ON T.SalesKey = S.SalesKey
            WHEN MATCHED THEN
                UPDATE SET
                    T.channel        = S.channel,
                    T.DateKey        = S.DateKey,
                    T.DiscountAmount = S.DiscountAmount,
                    T.DiscountQuantity = S.DiscountQuantity,
                    T.Product_ID     = S.Product_ID,
                    T.PromotionKey   = S.PromotionKey,
                    T.ReturnAmount   = S.ReturnAmount,
                    T.ReturnQuantity = S.ReturnQuantity,
                    T.SalesAmount    = S.SalesAmount,
                    T.SalesQuantity  = S.SalesQuantity,
                    T.StoreKey       = S.StoreKey,
                    T.TotalCost      = S.TotalCost,
                    T.UnitCost       = S.UnitCost,
                    T.UnitPrice      = S.UnitPrice
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    SalesKey,
                    channel,
                    DateKey,          -- INT
                    DiscountAmount,
                    DiscountQuantity,
                    Product_ID,
                    PromotionKey,
                    ReturnAmount,
                    ReturnQuantity,
                    SalesAmount,
                    SalesQuantity,
                    StoreKey,
                    TotalCost,
                    UnitCost,
                    UnitPrice
                )
                VALUES (
                    S.SalesKey,
                    S.channel,
                    S.DateKey,
                    S.DiscountAmount,
                    S.DiscountQuantity,
                    S.Product_ID,
                    S.PromotionKey,
                    S.ReturnAmount,
                    S.ReturnQuantity,
                    S.SalesAmount,
                    S.SalesQuantity,
                    S.StoreKey,
                    S.TotalCost,
                    S.UnitCost,
                    S.UnitPrice
                );

        END;
        """
    )

def parse_decimal(value):
    """Convierte valores decimales con coma a punto"""
    if not value or value.strip() == "":
        return None
    return float(value.replace(",", "."))


def parse_int(value):
    """Convierte valores enteros, maneja valores vacíos"""
    if not value or value.strip() == "":
        return None
    return int(float(value))


def parse_date(value):
    """Convierte fecha en formato DD/MM/YYYY a datetime"""
    if not value or value.strip() == "":
        return None
    try:
        # Intenta varios formatos de fecha - retorna datetime en lugar de date
        for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y", "%Y-%m-%d"]:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None
    except:
        return None


def load_channel(cursor, csv_path):
    """Carga datos de Channel"""
    print(f"\nCargando Channel desde {csv_path.name}...")
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cursor.callproc(
                f"{SQL_SERVER_CONFIG['schema']}.sp_InsertChannel",
                (parse_int(row["ChannelID"]), row["ChannelName"]),
            )


def load_geography(cursor, csv_path):
    """Carga datos de Geography"""
    print(f"\nCargando Geography desde {csv_path.name}...")
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cursor.callproc(
                f"{SQL_SERVER_CONFIG['schema']}.sp_InsertGeography",
                (
                    parse_int(row["GeographyKey"]),
                    row["ContinentName"],
                    row["GeographyType"],
                    row["RegionCountryName"],
                ),
            )


def load_product_category(cursor, csv_path):
    """Carga datos de ProductCategory"""
    print(f"\nCargando ProductCategory desde {csv_path.name}...")
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cursor.callproc(
                f"{SQL_SERVER_CONFIG['schema']}.sp_InsertProductCategory",
                (parse_int(row["ProductCategoryKey"]), row["ProductCategory"]),
            )


def load_product_subcategory(cursor, csv_path):
    """Carga datos de ProductSubcategory"""
    print(f"\nCargando ProductSubcategory desde {csv_path.name}...")
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cursor.callproc(
                f"{SQL_SERVER_CONFIG['schema']}.sp_InsertProductSubcategory",
                (
                    parse_int(row["ProductSubcategoryKey"]),
                    row["ProductSubcategory"],
                    parse_int(row["ProductCategoryKey"]),
                ),
            )


def load_product(cursor, csv_path):
    """Carga datos de Product"""
    print(f"\nCargando Product desde {csv_path.name}...")
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cursor.callproc(
                f"{SQL_SERVER_CONFIG['schema']}.sp_InsertProduct",
                (
                    parse_int(row["ProductKey"]),
                    row["ProductName"],
                    row["BrandName"],
                    row["ClassName"],
                    row["Manufacturer"],
                    row["ProductDescription"],
                    parse_int(row["ProductSubcategoryKey"]),
                    parse_decimal(row["UnitCost"]),
                    parse_decimal(row["UnitPrice"]),
                ),
            )


def load_promotion(cursor, csv_path):
    """Carga datos de Promotion"""
    print(f"\nCargando Promotion desde {csv_path.name}...")
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cursor.callproc(
                f"{SQL_SERVER_CONFIG['schema']}.sp_InsertPromotion",
                (
                    parse_int(row["PromotionKey"]),
                    row["PromotionName"],
                    row["PromotionLabel"],
                    parse_decimal(row["DiscountPercent"]),
                    parse_date(row["StartDate"]),
                    parse_date(row["EndDate"]),
                    parse_int(row["Priority"]),
                ),
            )


def load_stores(cursor, csv_path):
    """Carga datos de Stores"""
    print(f"\nCargando Stores desde {csv_path.name}...")
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cursor.callproc(
                f"{SQL_SERVER_CONFIG['schema']}.sp_InsertStores",
                (
                    parse_int(row["StoreKey"]),
                    parse_int(row["GeographyKey"]),
                    row["StoreName"],
                    row["StoreType"],
                    row["Status"],
                    parse_int(row["SellingAreaSize"]),
                    parse_int(row["EmployeeCount"]),
                    row["CloseReason"] if row["CloseReason"] else None,
                ),
            )


def load_sales(cursor, csv_path):
    """Carga datos de Sales"""
    print(f"\nCargando Sales desde {csv_path.name}...")
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:

            cursor.callproc(
                f"{SQL_SERVER_CONFIG['schema']}.sp_InsertSales",
                (
                    parse_int(row["SalesKey"]),
                    parse_date(row["DateKey"]),
                    parse_int(row["channel"]),
                    parse_int(row["StoreKey"]),
                    parse_int(row["Product_ID"]),
                    parse_int(row["PromotionKey"]),
                    parse_int(row["SalesQuantity"]),
                    parse_decimal(row["SalesAmount"]),
                    parse_int(row["ReturnQuantity"]),
                    parse_decimal(row["ReturnAmount"]),
                    parse_int(row["DiscountQuantity"]),
                    parse_decimal(row["DiscountAmount"]),
                    parse_decimal(row["TotalCost"]),
                    parse_decimal(row["UnitCost"]),
                    parse_decimal(row["UnitPrice"]),
                ),
            )
    
def excute_migration(cursor):
    """Ejecuta el proceso de migración de datos"""
    cursor.execute(f"EXEC {SQL_SERVER_CONFIG['schema']}.MigrateDB")


def main(DATA_DIR):
    """Función principal de carga de datos"""
    try:
        # Conectar a la base de datos
        conn = pymssql.connect(
            server=SQL_SERVER_CONFIG["server"],
            port=SQL_SERVER_CONFIG["port"],
            user=SQL_SERVER_CONFIG["user"],
            password=SQL_SERVER_CONFIG["password"],
            database=SQL_SERVER_CONFIG["database"],
            autocommit=True,
        )
        cursor = conn.cursor()

        # 0. Crear procedimientos almacenados
        create_processes(cursor)

        # 1. Tablas sin dependencias
        load_channel(cursor, DATA_DIR / "Channel.csv")
        load_geography(cursor, DATA_DIR / "Geography.csv")
        load_product_category(cursor, DATA_DIR / "ProductCategory.csv")

        # 2. Tablas con dependencias
        load_product_subcategory(cursor, DATA_DIR / "ProductSubcategory.csv")
        load_product(cursor, DATA_DIR / "Product.csv")
        load_promotion(cursor, DATA_DIR / "Promotion.csv")
        load_stores(cursor, DATA_DIR / "Stores.csv")
        load_sales(cursor, DATA_DIR / "Sales.csv")

        # Mostrar resumen
        print("\nRESUMEN DE CARGA")

        tables = [
            "Channel",
            "Geography",
            "ProductCategory",
            "ProductSubcategory",
            "Product",
            "Promotion",
            "Stores",
            "Sales",
        ]

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM SQLBI.{table}")
            count = cursor.fetchone()[0]
            print(f"   {table:25} → {count:>7} registros")

        # 3. Crear proceso de migración
        create_migration_process(cursor)

        # Cerrar conexión
        conn.close()
        print("\nCarga de datos completada exitosamente.")
    except FileNotFoundError as e:
        print(f"\nError: Archivo CSV no encontrado - {e}")
        sys.exit(1)
    except pymssql.Error as e:
        print(f"\nError de base de datos: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
