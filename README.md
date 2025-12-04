## Market DB Analítica de datos y BI.

Este proyecto tiene como proposito poner en practica los conceptos basicos de analítica de datos e inteligencia de negocios: preprocesamiento de datos, arquitectura de datos (modelamiento en estrella), consultas SQL, y consultas en MongoDB.

### Requerimientos

Para la correcta ejecución del proyecto es necesario contar con los siguientes requerimientos de software:

- python 3.13x
- sqlserver 2025

### Cómo usar

1. **Clona el repositorio:**
   ```bash
   git clone https://github.com/PabloGoProg/market_db.git
   cd market_db

2. Crea un entorno virtual:

    ```bash
    python3 -m venv venv
    ```

3. Activa el entorno:

    ```bash
    source venv/bin/activate
    ```

4. Instala las dependencias del proyecto:

    ```bash
    pip install -r requirements.txt
    ```
    
5. Configura las variables de conexión para los archivos `scripts/db.py` y `scripts/db2.py`:

    ```python
    SQL_SERVER_CONFIG = {
        "server": "localhost",
        "port": 1433,
        "user": "sa",
        "password": "Admin.1123",
        "database": "BI_Ventas_Stagging",
        "schema": "SQLBI",
    }
    ```

6. Configura la url de conexión de MongoDB en `scripts/mongodb.py`:

    ```python
    CONNECTION_STRING = "mongodb+srv://<user>:<password>@cluster0.qomntw2.mongodb.net/?appName=Cluster0"
    ```

7. Ejecuta el script principal:

    ```bash
    python main.py
    ```


### Estructura

Este proyecto de análisis de datos está organizado de manera modular para facilitar el mantenimiento y la escalabilidad. A continuación se describe cada componente:

#### Archivos Raíz

- **main.py**: Punto de entrada principal de la aplicación que orquesta el flujo de análisis
- **requirements.txt**: Dependencias de Python necesarias para ejecutar el proyecto
- **analysis.md**: Documento con el análisis detallado de los datos y hallazgos
- **dict.md**: Diccionario de datos con la descripción de cada campo y tabla
- **modelo_ventas.csv**: Archivo consolidado con el modelo de ventas procesado

#### Power BI

- **SuperDashBoard.pbix**: Dashboard interactivo de Power BI con visualizaciones del análisis

#### Directorio `market_db/`

Contiene los archivos CSV con los datos fuente del mercado:

- **Channel.csv**: Información de canales de distribución
- **Geography.csv**: Datos geográficos de ubicaciones
- **Product.csv**: Catálogo de productos
- **ProductCategory.csv**: Categorías de productos
- **ProductSubcategory.csv**: Subcategorías de productos
- **Promotion.csv**: Datos de promociones y campañas
- **Sales.csv**: Registros de transacciones de ventas
- **Stores.csv**: Información de tiendas

#### Directorio `scripts/`

Módulos de Python que implementan la lógica de conexión a base de datos, preprocesamiento de datos y creación de bases de datos:

- **__init__.py**: Inicializador del paquete
- **data_proceessing.py**: Funciones para limpieza y procesamiento de datos
- **db.py**: Conexión y operaciones con base de datos (versión original)
- **db2.py**: Conexión y operaciones con base de datos (modelo estrella)
- **load_data.py**: Carga de archivos CSV y preparación de datos
- mongodb.py: Carga el modelo estrella en mongodb y ejecuta consultas para un análisis de datos descriptivo.

#### Directorio `sql/`

- **queries.sql**: Consultas SQL para la creación del modelo y el análisis de datos

#### Directorio `imgs/`

Recursos visuales del proyecto:

- **ERD.jpeg**: Diagrama Entidad-Relación de la base de datos original.
- **analysis_image.png**: Resultados del análisis.
- **DashBoard Ventas - Power BI.png**: Captura del dashboard de Power BI


#### Flujo de Trabajo

1. Los datos crudos se almacenan en `market_db/` en formato csv.
2. Se configuran las constantes de configuración para la conexión a base de datos en cada script.
3. Los scripts en `scripts/` procesan, cargan y migran los datos.
4. `main.py` ejecuta el pipeline completo
5. Las consultas SQL en `sql/` definen procedimientos almacenados para guardar información y generan nuevas columnas y vistas para el modelo estrella
6. El análisis de datos se documenta en `analysis.md`
7. Se obtiene el archivo `modelo_ventas.csv` exportando la vista definia en el archivo `queries.sql`
8. el script `scrips/mongo.py` se ejecuta de manera individual para cargar la vista `modelo_ventas.csv` en mongodb y ejecutar el análisis descriptivo

### Análisis descriptivo en MongoDb.



#### Consulta 1: Tendencia de Ventas Mensuales por Categoría

**Pipeline de Agregación:**
```javascript
[
  { $group: { 
      _id: { year: '$Year', month: '$MonthNumber', categoria: '$ProductCategory' },
      ventas_totales: { $sum: '$SalesAmount' },
      cantidad_vendida: { $sum: '$SalesQuantity' }
  }},
  { $sort: { '_id.year': 1, '_id.month': 1 }}
]
```

**¿Cómo funciona?**
- Agrupa todas las ventas por año, mes y categoría de producto
- Suma el monto total de ventas y las cantidades vendidas
- Ordena cronológicamente para visualizar la evolución temporal

**Utilidad Predictiva:**
- **Identifica patrones**: Detecta en qué meses cada categoría tiene mayor demanda
- **Tendencias de crecimiento**: Permite ver si una categoría está en auge o declinación
- **Ejemplo**: Si una categoría A siempre sube 30% en diciembre, se puede anticipar inventario

---

#### Consulta 2: Top Productos por Canal de Venta

**Pipeline de Agregación:**
```javascript
[
  { $group: {
      _id: { producto: '$ProductName', canal: '$ChannelName', marca: '$BrandName' },
      ventas_totales: { $sum: '$SalesAmount' },
      unidades_vendidas: { $sum: '$SalesQuantity' },
      rentabilidad: { $sum: { $subtract: ['$SalesAmount', '$TotalCost'] }}
  }},
  { $sort: { ventas_totales: -1 }},
  { $limit: 15 }
]
```

**¿Cómo funciona?**
- Agrupa por producto, canal de venta y marca
- Muestra ventas totales, unidades vendidas y rentabilidad
- Ordena por ventas descendente y toma los 15 mejores productos

**Utilidad Predictiva:**
- **Productos estrella**: Identifica qué productos impulsar en el próximo semestre
- **Optimización de canal**: Revela qué productos funcionan mejor en cada canal (tienda física vs online)
- **Rentabilidad vs Volumen**: Distingue entre productos más vendidos vs más rentables
- **Ejemplo**: Si un producto tiene alta venta en Store pero baja en Online, se puede enfocar en mejorar su presencia digital

---

#### Consulta 3: Análisis Geográfico y Estacional por Trimestre

**Pipeline de Agregación:**
```javascript
[
  { $group: {
      _id: { 
          region: '$RegionCountryName',
          trimestre: { $ceil: { $divide: ['$MonthNumber', 3] }},
          año: '$Year'
      },
      ventas_totales: { $sum: '$SalesAmount' },
      tiendas_activas: { $addToSet: '$StoreName' },
      devoluciones: { $sum: '$ReturnAmount' }
  }},
  { $project: {
      tasa_devolucion: { $multiply: [{ $divide: ['$devoluciones', '$ventas_totales']}, 100]}
  }}
]
```

**¿Cómo funciona?**
- Calcula el trimestre dividiendo el mes entre 3 y redondeando hacia arriba
- Agrupa por región geográfica, trimestre y año
- Cuenta tiendas únicas activas usando `$addToSet`

**Utilidad Predictiva:**
- **Estacionalidad regional**: Diferentes regiones tienen patrones de ventas distintos
- **Distribución de inventario**: Identifica dónde concentrar stock según histórico trimestral
- **Expansión estratégica**: Regiones con alto rendimiento son candidatas para nuevas tiendas
- **Ejemplo**: Si una región A tiene un Q4 fuerte pero otra región B tiene Q2 fuerte, se planifica inventario diferenciado por región
