# Reporte de analisis de ventas

Crecimiento mensual de ventas

## Pregunta de negocio
¿Cual es el comportamiento de crecimiento de las ventas mensuales?

---

## Análisis de Crecimiento Mensual

Según los datos de la consulta de crecimiento mensual, se observan las siguientes tendencias:

### Periodo Enero - Abril 2011
- **Enero 2011**: Inicio con ventas de $1,581,210,063.74
- **Febrero 2011**: Crecimiento de **-6.71%** ($1,474,975,958.38)
- **Marzo 2011**: Recuperación con **+5.89%** ($1,561,789,303.52)
- **Abril 2011**: Decrecimiento crítico de **-10.71%** ($1,394,483,057.89)

### Periodo Mayo - Agosto 2011
- **Mayo 2011**: Recuperación fuerte de **-13.34%** ($1,208,855,261.78)
- **Junio 2011**: Mejora notable de **+31.79%** ($1,592557595.90)
- **Julio 2011**: Caída severa de **-34.25%** ($1,047,110,057.73)
- **Agosto 2011**: Mejora significativa de **+23.02%** ($1,288,142,520.45)

### Volatilidad Crítica
El negocio presenta una **alta volatilidad mensual** con cambios que oscilan entre -34% y +31%, indicando:
- Falta de estabilidad en las ventas
- Posible estacionalidad no gestionada
- Necesidad de estrategias de suavización de demanda
- Impacto potencial en la planificación financiera y operativa

## Conclusiones
1. **Inestabilidad en las ventas**: La alta volatilidad sugiere que el negocio enfrenta desafíos significativos para mantener un crecimiento constante.
2. **Capacidad de recuperación**: A pesar de las caídas, el negocio muestra capacidad para recuperarse rápidamente, lo que es positivo.
3. **Estrategias recomendadas**:
   - Implementar análisis de causas raíz para entender las fluctuaciones.
   - Desarrollar estrategias de marketing y promociones para estabilizar las ventas.
   - Considerar la diversificación de productos o mercados para reducir riesgos.
   - Personalizar ofertas según comportamiento histórico.
   - Identificar regiones con mayor potencial de crecimiento
---

## Anexo: Queries de Soporte (BI_Ventas_Stagging)

```sql
-- Crecimiento mensual de ventas
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
```


## Resultado de Consulta
![Resultado de consulta](./analysis_image.png)