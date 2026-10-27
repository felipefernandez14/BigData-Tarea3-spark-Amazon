# Procesamiento de Datos en Batch con Apache Spark - Amazon E-Commerce (UNAD)

## 1. Descripción del Problema y Solución
En la última década, las ventas en línea (*e-commerce*) se han convertido en una opción muy popular gracias a la facilidad de adquirir productos sin necesidad de salir de casa. Sin embargo,
dependiendo de la ubicación geográfica, existen artículos que no tienen cobertura de envío o que llegan con un alto sobrecosto, una problemática que se evidencia especialmente en el sector de la tecnología.

Teniendo esto en cuenta, este proyecto implementa una solución en **Apache Spark (PySpark)** para un distribuidor mayorista que analiza 1 millón de registros históricos de ventas de Amazon
(`amazon_ecommerce_1M.csv`, obtenido de Kaggle). El script realiza limpieza de datos, filtra devoluciones, extrae los trimestres a partir de la fecha de compra (`purchase_date`) y agrupa las ventas por región 
(`location`), trimestre y categoría/subcategoría (`category`, `subcategory`), permitiendo identificar qué productos son los más cotizados en cada región y trimestre para planificar exportaciones al por mayor.

## 2. Fuente del Conjunto de Datos
* **Dataset:** Amazon E-Commerce Sales Dataset (1M rows)
* **Autor / Fuente:** Kaggle - `sharmajicoder/amazon-e-commerce`
* **Enlace:** https://www.kaggle.com/datasets/sharmajicoder/amazon-e-commerce

## 3. Instrucciones de Ejecución
1. Descargar y descomprimir el conjunto de datos en el entorno Linux con Apache Spark instalado:
   ```bash
   curl -L -o amazon-e-commerce.zip [https://www.kaggle.com/api/v1/datasets/download/sharmajicoder/amazon-e-commerce](https://www.kaggle.com/api/v1/datasets/download/sharmajicoder/amazon-e-commerce)
   unzip amazon-e-commerce.zip
   ```
2. Asegurarse de que el archivo `amazon_ecommerce_1M.csv` se encuentre en el mismo directorio que el script `analisis_amazon_batch.py`.
3. Ejecutar el procesamiento en Batch mediante `spark-submit`:
   ```bash
   spark-submit analisis_amazon_batch.py
   ```
