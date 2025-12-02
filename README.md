## Market DB Analítica de datos y BI.

Este proyecto tiene como proposito poner en practica los conceptos basicos de analítica de datos e inteligencia de negocios: preprocesamiento de datos, arquitectura de datos (modelamiento en estrella), consultas SQL, y consultas en MongoDB.

### Requerimientos

Para la correcta ejecución del proyecto es necesario contar con los siguientes requerimientos de software:

- python 3.13x
- sqlserver 202

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
    
5. Ejecuta el script principal:

    ```bash
    python main.py
    ```

### Estructura

Este proyecto de análisis de datos está organizado de manera modular para facilitar el mantenimiento y la escalabilidad. A continuación se describe cada componente:

#### Archivos Raíz

- **README.md**: Documentación principal del proyecto con instrucciones de uso e instalación
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

Módulos de Python que implementan la lógica del proyecto:

- **__init__.py**: Inicializador del paquete
- **data_proceessing.py**: Funciones para limpieza y procesamiento de datos
- **db.py**: Conexión y operaciones con base de datos (versión principal)
- **db2.py**: Conexión y operaciones con base de datos (versión alternativa)
- **load_data.py**: Carga de archivos CSV y preparación de datos
- **__pycache__/**: Archivos compilados de Python (generados automáticamente)

#### Directorio `sql/`

- **queries.sql**: Consultas SQL para extracción y análisis de datos

#### Directorio `imgs/`

Recursos visuales del proyecto:

- **DashBoard Ventas - Power BI.png**: Captura del dashboard de Power BI
- **ERD.jpeg**: Diagrama Entidad-Relación de la base de datos
- **analysis_image.png**: Imágenes de apoyo para el análisis

#### Directorio `venv/`

Entorno virtual de Python (no se incluye en control de versiones)

---

#### Flujo de Trabajo

1. Los datos crudos se almacenan en `market_db/`
2. Los scripts en `scripts/` procesan y cargan los datos
3. `main.py` ejecuta el pipeline completo
4. Los resultados se consolidan en `modelo_ventas.csv`
5. Las consultas SQL en `sql/` permiten análisis adicionales
6. Las visualizaciones se crean en `SuperDashBoard.pbix`
7. El análisis final se documenta en `analysis.md`
