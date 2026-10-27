from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, quarter, year, count, sum as _sum, round as _round, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# 1. Inicializar la sesión de Apache Spark para procesamiento en Batch
spark = SparkSession.builder \
    .appName("Analisis_Amazon_Batch_Mayorista") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n==================================================================")
print("1. CARGA DEL DATASET Y ANÁLISIS EXPLORATORIO DE DATOS (EDA)")
print("==================================================================")
ruta_csv = "amazon_ecommerce_1M.csv"
df = spark.read.csv(ruta_csv, header=True, inferSchema=True)

total_inicial = df.count()
print(f"-> Total de registros cargados: {total_inicial}")
print("-> Esquema de columnas del dataset:")
df.printSchema()

print("-> Muestra de los primeros 5 registros:")
df.select("product_id", "category", "subcategory", "final_price", "purchase_date", "location", "delivery_status").show(5)

print("\n==================================================================")
print("2. LIMPIEZA Y TRANSFORMACIÓN DE DATOS")
print("==================================================================")
# Eliminamos registros con valores nulos en las columnas clave y duplicados
columnas_clave = ["purchase_date", "location", "category", "subcategory", "final_price"]
df_limpio = df.dropna(subset=columnas_clave).dropDuplicates()

# Filtramos las órdenes que NO fueron devueltas (is_returned == False) para analizar ventas reales
df_ventas_reales = df_limpio.filter(col("is_returned") == False)

# Transformación: Convertimos purchase_date a fecha y extraemos Año y Trimestre (1, 2, 3 o 4)
df_transformado = df_ventas_reales \
    .withColumn("fecha", to_date(col("purchase_date"), "yyyy-MM-dd")) \
    .withColumn("anio", year(col("fecha"))) \
    .withColumn("trimestre", quarter(col("fecha")))

print(f"-> Total de ventas efectivas limpias (sin devoluciones): {df_transformado.count()}")

print("\n==================================================================")
print("3. RESULTADOS: TENDENCIA DE PRODUCTOS MÁS VENDIDOS POR TRIMESTRE Y REGIÓN")
print("==================================================================")
# Agrupamos por Región (location), Trimestre, Categoría y Subcategoría
tendencia_regional = df_transformado.groupBy("location", "trimestre", "category", "subcategory") \
    .agg(
        count("*").alias("unidades_vendidas"),
        _round(_sum("final_price"), 2).alias("ingresos_totales")
    )

# Mostramos el Top 20 general ordenado por Trimestre y mayor cantidad de unidades vendidas
print("-> Top 20 de Productos/Subcategorías más vendidos por Región y Trimestre:")
tendencia_regional.orderBy(col("trimestre").asc(), col("unidades_vendidas").desc()).show(20, truncate=False)

# Calculamos el Producto #1 más cotizado de cada Región en cada Trimestre para comparar Región 1 vs Región 2
ventana_ranking = Window.partitionBy("trimestre", "location").orderBy(col("unidades_vendidas").desc())
top1_por_region = tendencia_regional.withColumn("ranking", row_number().over(ventana_ranking)) \
    .filter(col("ranking") == 1) \
    .orderBy("trimestre", "location")

print("-> Comparativa Mayorista: El Producto #1 más vendido en cada Región por Trimestre:")
top1_por_region.select("trimestre", "location", "category", "subcategory", "unidades_vendidas", "ingresos_totales").show(30, truncate=False)

# Guardamos el resultado procesado en una carpeta CSV para evidencia
top1_por_region.coalesce(1).write.mode("overwrite").option("header", "true").csv("resultados_mayorista_amazon")
print("-> Resultados exportados exitosamente en la carpeta: 'resultados_mayorista_amazon'")

spark.stop()
