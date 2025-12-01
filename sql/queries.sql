-- IMPORTAR DATOS SQL SERVER

-- Procedimientos Almacenados para BI_Ventas_Stagging

-- SP: Insertar Channel
CREATE OR ALTER PROCEDURE SQLBI.sp_InsertChannel
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
GO

-- SP: Insertar Geography
CREATE OR ALTER PROCEDURE SQLBI.sp_InsertGeography
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
GO

-- SP: Insertar ProductCategory
CREATE OR ALTER PROCEDURE SQLBI.sp_InsertProductCategory
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
GO

-- SP: Insertar ProductSubcategory
CREATE OR ALTER PROCEDURE SQLBI.sp_InsertProductSubcategory
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
GO

-- SP: Insertar Product
CREATE OR ALTER PROCEDURE SQLBI.sp_InsertProduct
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
GO

-- SP: Insertar Promotion
CREATE OR ALTER PROCEDURE SQLBI.sp_InsertPromotion
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
GO

-- SP: Insertar Stores
CREATE OR ALTER PROCEDURE SQLBI.sp_InsertStores
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
GO

-- SP: Insertar Sales
CREATE OR ALTER PROCEDURE SQLBI.sp_InsertSales
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
GO

-- Conteo de Valores nulos por tabla y columna
DECLARE @TableName SYSNAME = 'SQLBI.Geography';
DECLARE @SQL       NVARCHAR(MAX) = N'';

SELECT 
    @SQL = @SQL + '
SELECT 
    ''' + c.name + ''' AS ColumnName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN [' + c.name + '] IS NULL THEN 1 ELSE 0 END) AS NullCount
FROM ' + @TableName + '
UNION ALL'
FROM sys.columns c
JOIN sys.objects o 
    ON c.object_id = o.object_id
WHERE 
    o.object_id = OBJECT_ID(@TableName);

SET @SQL = LEFT(@SQL, LEN(@SQL) - LEN('UNION ALL'));
EXEC sys.sp_executesql @SQL;

SET @TableName = 'SQLBI.Channel';
EXEC sys.sp_executesql @SQL;

SET @TableName = 'SQLBI.Promotion';
EXEC sys.sp_executesql @SQL;

SET @TableName = 'SQLBI.ProductCategory';
EXEC sys.sp_executesql @SQL;

SET @TableName = 'SQLBI.ProductSubcategory';
EXEC sys.sp_executesql @SQL;

SET @TableName = 'SQLBI.Product';
EXEC sys.sp_executesql @SQL;

SET @TableName = 'SQLBI.Stores';
EXEC sys.sp_executesql @SQL;

-- Generar diccionario de datos
-- Tabla con columnas: Tabla, Columna, Tipo de Dato, Longitud, Permite Nulos, Descripción
-- Comando para generar:
-- sqlcmd -S localhost -d BI_Ventas_Stagging -E -i queries.sql -o DataDictionary.md
SELECT
    '| ' + t.name +
    ' | ' + c.name +
    ' | ' + ty.name +
    ' | ' + CAST(c.max_length AS VARCHAR(10)) +
    ' | ' + CASE WHEN c.is_nullable = 1 THEN 'Sí' ELSE 'No' END +
    ' | ' + ISNULL(CAST(ep.value AS NVARCHAR(MAX)), '') + ' |'
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.types ty ON c.user_type_id = ty.user_type_id
LEFT JOIN sys.extended_properties ep
    ON ep.major_id = t.object_id
    AND ep.minor_id = c.column_id
    AND ep.name = 'MS_Description'
ORDER BY t.name, c.name;



-- TRANSFORMAR Y MODELAMIENTO DE DATOS

-- Migración Base de datos

-- Procedimiento de transición para tabla Channel
CREATE OR ALTER PROCEDURE SQLBI.MigrateDB
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

-- ANALISIS DEL NEGOCIO

-- Total de ventas por tienda
CREATE OR ALTER VIEW SQLBI.vw_VentasPorTienda AS
SELECT
    s.StoreKey,
    st.StoreName,
    SUM(s.SalesAmount) AS TotalSales
FROM SQLBI.Sales AS s
INNER JOIN SQLBI.Stores AS st
    ON s.StoreKey = st.StoreKey
GROUP BY 
    s.StoreKey,
    st.StoreName
ORDER BY 
    TotalSales DESC;

-- Listar los productos más vendidos
CREATE OR ALTER VIEW SQLBI.vw_ProductosMasVendidos AS
SELECT
    p.ProductKey,
    p.ProductName,
    SUM(s.SalesQuantity) AS TotalQuantity,
    SUM(s.SalesAmount)  AS TotalSales
FROM SQLBI.Sales AS s
INNER JOIN SQLBI.Product AS p
    ON s.Product_ID = p.ProductKey
GROUP BY 
    p.ProductKey,
    p.ProductName
ORDER BY 
    TotalQuantity DESC;

-- Ventas realizadas por año
CREATE OR ALTER VIEW SQLBI.vw_VentasPorAnio AS
SELECT 
    YEAR(s.DateKey) AS [Year],
    SUM(s.SalesAmount) AS TotalSales
FROM SQLBI.Sales AS s
WHERE s.DateKey IS NOT NULL
  AND s.SalesAmount IS NOT NULL
GROUP BY 
    YEAR(s.DateKey)
ORDER BY 
    [Year];

-- Ventas por categoría de producto
CREATE OR ALTER VIEW SQLBI.vw_VentasPorCategoria AS
SELECT 
    pc.ProductCategory AS Category,
    SUM(s.SalesAmount) AS TotalSales
FROM SQLBI.Sales AS s
INNER JOIN SQLBI.Product AS p
    ON s.Product_ID = p.ProductKey
INNER JOIN SQLBI.ProductSubcategory AS psc
    ON p.ProductSubcategoryKey = psc.ProductSubcategoryKey
INNER JOIN SQLBI.ProductCategory AS pc
    ON psc.ProductCategoryKey = pc.ProductCategoryKey
GROUP BY 
    pc.ProductCategory
ORDER BY 
    TotalSales DESC;

-- Ventas promedio por canal
CREATE OR ALTER VIEW SQLBI.vw_VentasPromedioPorCanal AS
SELECT 
    c.ChannelID,
    c.ChannelName,
    AVG(ISNULL(s.SalesAmount, 0)) AS AvgSalesAmount
FROM SQLBI.Channel AS c
LEFT JOIN SQLBI.Sales AS s
    ON s.channel = c.ChannelID
GROUP BY 
    c.ChannelID,
    c.ChannelName
ORDER BY 
    AvgSalesAmount DESC;

-- Tiendas con ventas mayores al promedio general
CREATE OR ALTER VIEW SQLBI.vw_TiendasSobrePromedio AS
WITH StoreSales AS (
    SELECT 
        s.StoreKey,
        st.StoreName,
        SUM(s.SalesAmount) AS TotalSales
    FROM SQLBI.Sales AS s
    INNER JOIN SQLBI.Stores AS st
        ON s.StoreKey = st.StoreKey
    GROUP BY 
        s.StoreKey,
        st.StoreName
),
GlobalAvg AS (
    SELECT 
        AVG(TotalSales) AS AvgStoreSales
    FROM StoreSales
)
SELECT 
    ss.StoreKey,
    ss.StoreName,
    ss.TotalSales
FROM StoreSales AS ss
CROSS JOIN GlobalAvg AS g
WHERE ss.TotalSales > g.AvgStoreSales
ORDER BY 
    ss.TotalSales DESC;

-- Ventas por país y mes
CREATE OR ALTER VIEW SQLBI.vw_VentasPorPaisYMes AS
SELECT 
    g.RegionCountryName AS Country,
    YEAR(s.DateKey)      AS [Year],
    DATENAME(MONTH, s.DateKey) AS [Month],
    SUM(s.SalesAmount)   AS TotalSales
FROM SQLBI.Sales AS s
INNER JOIN SQLBI.Stores AS st
    ON s.StoreKey = st.StoreKey
INNER JOIN SQLBI.Geography AS g
    ON st.GeographyKey = g.GeographyKey
WHERE 
    s.DateKey IS NOT NULL
    AND s.SalesAmount IS NOT NULL
    AND g.RegionCountryName IS NOT NULL
GROUP BY 
    g.RegionCountryName,
    YEAR(s.DateKey),
    DATENAME(MONTH, s.DateKey),
    MONTH(s.DateKey)
ORDER BY 
    g.RegionCountryName,
    [Year],
    MONTH(s.DateKey);

-- Top 5 productos más vendidos por categoría
CREATE OR ALTER VIEW SQLBI.vw_Top5ProductosPorCategoria AS
WITH ProductCategorySales AS (
    SELECT 
        pc.ProductCategory,
        p.ProductKey,
        p.ProductName,
        SUM(s.SalesQuantity) AS TotalQuantity,
        SUM(s.SalesAmount)   AS TotalSales,
        ROW_NUMBER() OVER (
            PARTITION BY pc.ProductCategory
            ORDER BY SUM(s.SalesQuantity) DESC
        ) AS rn
    FROM SQLBI.Sales AS s
    INNER JOIN SQLBI.Product AS p
        ON s.Product_ID = p.ProductKey
    INNER JOIN SQLBI.ProductSubcategory AS psc
        ON p.ProductSubcategoryKey = psc.ProductSubcategoryKey
    INNER JOIN SQLBI.ProductCategory AS pc
        ON psc.ProductCategoryKey = pc.ProductCategoryKey
    GROUP BY 
        pc.ProductCategory,
        p.ProductKey,
        p.ProductName
)
SELECT 
    ProductCategory,
    ProductKey,
    ProductName,
    TotalQuantity,
    TotalSales
FROM ProductCategorySales
WHERE rn <= 5
ORDER BY 
    ProductCategory,
    rn;

-- Crecimiento mensual de ventas
CREATE OR ALTER VIEW SQLBI.vw_CrecimientoMensualVentas AS
WITH MonthlySales AS (
    SELECT 
        YEAR(DateKey)  AS [Year],
        MONTH(DateKey) AS [Month],
        SUM(SalesAmount) AS TotalSales
    FROM SQLBI.Sales
    WHERE
        DateKey IS NOT NULL
    GROUP BY 
        YEAR(DateKey),
        MONTH(DateKey)
),
MonthlyGrowth AS (
    SELECT 
        [Year],
        [Month],
        TotalSales,
        LAG(TotalSales) OVER (ORDER BY [Year], [Month]) AS PrevMonthSales
    FROM MonthlySales
)
SELECT 
    [Year],
    [Month],
    TotalSales,
    PrevMonthSales,
    CASE 
        WHEN PrevMonthSales IS NULL OR PrevMonthSales = 0 
             THEN NULL
        ELSE (TotalSales - PrevMonthSales) / PrevMonthSales * 100.0
    END AS GrowthPercent
FROM MonthlyGrowth
ORDER BY 
    [Year],
    [Month];

-- Ventas ponderadas por descuento aplicado
-- Ventas netas considerando el descuento aplicado sobre los productos de una promoción
CREATE OR ALTER VIEW SQLBI.vw_VentasPonderadasDescuento AS
SELECT 
    pr.PromotionKey,
    pr.PromotionName,
    SUM(s.SalesAmount) AS GrossSales,
    SUM(s.SalesAmount * (1.0 - pr.DiscountPercent)) AS NetSalesWeightedByDiscount
FROM SQLBI.Sales AS s
INNER JOIN SQLBI.Promotion AS pr
    ON s.PromotionKey = pr.PromotionKey
GROUP BY 
    pr.PromotionKey,
    pr.PromotionName
ORDER BY 
    NetSalesWeightedByDiscount DESC;

-- CARGUE (BI_Ventas_DWH)

-- Genera una columna nueva “StoreName_limpio” donde se elimine la palabra “Contoso” de la columna StoreName en la tabla Stores.
ALTER TABLE SQLBI.Stores
ADD StoreName_limpio AS REPLACE(StoreName, 'Contoso', '') PERSISTED;

-- Genera una métrica nueva “coste” que calcule la suma de los costes totales de las Ventas (TotalCost de Sales) 
SELECT 
    SUM(TotalCost) AS coste
FROM SQLBI.Sales;

-- Genera la métrica “productosDistintos” que calcule el número de productos distintos en la tabla Product 
SELECT 
    COUNT(DISTINCT ProductKey) AS productosDistintos
FROM SQLBI.Product;

-- Genera la métrica “canales” que calcule el número de filas en la tabla canales Data 
SELECT 
    COUNT(*) AS canales
FROM SQLBI.Channel;

-- Genera la columna calculada “Rentabilidad” que devuelva si es rentable (ReturnAmount>=100) o si no es rentable (ReturnAmount<100) en la dimension Sales.
ALTER TABLE SQLBI.Sales
ADD Rentabilidad AS 
    CASE 
        WHEN ReturnAmount >= 100 THEN 'Rentable'
        ELSE 'No Rentable'
    END PERSISTED;

-- Genera la columna calculada “canalPersonalizado” que devuelva una concatenación de “Canal “y la columna ChannelName dentro de la tabla Channel
ALTER TABLE SQLBI.Channel
ADD canalPersonalizado AS ('Canal ' + ChannelName) PERSISTED;

-- Cree una vista en SQL con la unificación de todos los campos llámela modelos_ventas
CREATE OR ALTER VIEW SQLBI.modelos_ventas AS
SELECT 
    -- Sales (Fact Table)
    s.SalesKey,
    s.SalesQuantity,
    s.SalesAmount,
    s.ReturnQuantity,
    s.ReturnAmount,
    s.DiscountQuantity,
    s.DiscountAmount,
    s.TotalCost,
    s.UnitCost,
    s.UnitPrice,
    s.Rentabilidad,
    
    -- Calendar
    c.DateKey,
    c.FullDate,
    c.DayOfMonth,
    c.DayOfWeek,
    c.DayOfYear,
    c.MonthNumber,
    c.MonthName,
    c.Year,
    c.IsHoliday,
    
    -- Channel
    ch.ChannelID,
    ch.ChannelName,
    ch.canalPersonalizado,
    
    -- Stores
    st.StoreKey,
    st.StoreName,
    st.StoreName_limpio,
    st.StoreType,
    st.Status,
    st.SellingAreaSize,
    st.EmployeeCount,
    st.CloseReason,
    
    -- Geography
    g.GeographyKey,
    g.ContinentName,
    g.GeographyType,
    g.RegionCountryName,
    
    -- Product
    p.ProductKey,
    p.ProductName,
    p.BrandName,
    p.ClassName,
    p.ColorName,
    p.Manufacturer,
    p.ProductDescription,
    
    -- ProductSubcategory
    ps.ProductSubcategoryKey,
    ps.ProductSubcategory,
    
    -- ProductCategory
    pc.ProductCategoryKey,
    pc.ProductCategory,
    
    -- Promotion
    pr.PromotionKey,
    pr.PromotionName,
    pr.PromotionLabel,
    pr.PromotionDescription,
    pr.DiscountPercent,
    pr.PromotionCategory,
    pr.PromotionType,
    pr.StartDate,
    pr.EndDate,
    pr.MinQuantity,
    pr.MaxQuantity,
    pr.Priority
    
FROM SQLBI.Sales s
LEFT JOIN SQLBI.Calendar c ON s.DateKey = c.DateKey
LEFT JOIN SQLBI.Channel ch ON s.channel = ch.ChannelID
LEFT JOIN SQLBI.Stores st ON s.StoreKey = st.StoreKey
LEFT JOIN SQLBI.Geography g ON st.GeographyKey = g.GeographyKey
LEFT JOIN SQLBI.Product p ON s.Product_ID = p.ProductKey
LEFT JOIN SQLBI.ProductSubcategory ps ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
LEFT JOIN SQLBI.ProductCategory pc ON ps.ProductCategoryKey = pc.ProductCategoryKey
LEFT JOIN SQLBI.Promotion pr ON s.PromotionKey = pr.PromotionKey;