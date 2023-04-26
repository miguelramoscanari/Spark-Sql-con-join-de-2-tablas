# Spark Sql con join de 2 tablas, usando Databrics.
Spark Sql con join de 2 tablas, usando Databrics Community que es gratis.

# Contenido
- "spark_miguel.ipynb" : notebook con el codigo fuente
- "cliente.csv", y "venta.csv": fuente de datos

# Databrics 
Debe usar Databrics community edition que es gratuito
- Crear un cluster, usando Compute
- Abrier notebook "spark_miguel.ipynb", usando Worrksapce / import / File
- En el Notebook, usar menu File / Upload data DBFS para añair "cliente.csv", y "venta.csv"

# Explicacion del codigo
Usando Spark, creamos un dataframe

```
dfCliente = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/tacnampt@gmail.com/cliente.csv")
dfVenta = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/tacnampt@gmail.com/venta.csv")
```

Para ver le esquema del dataframe
```
dfVenta.printSchema()
```

```
root
 |-- IdVenta: string (nullable = true)
 |-- Fecha: date (nullable = true)
 |-- Fecha_Entrega: string (nullable = true)
 |-- IdCanal: string (nullable = true)
 |-- IdCliente: integer (nullable = true)
 |-- IdSucursal: string (nullable = true)
 |-- IdEmpleado: string (nullable = true)
 |-- IdProducto: string (nullable = true)
 |-- Precio: decimal(10,2) (nullable = true)
 |-- Cantidad: integer (nullable = true)
```

Observamos que "Precio" y "Cantidad" es de tipo String, debemos cambiarlo a numerico. El campo "Fecha" es tipo String, debemos cambiarlo a tipo Date. Usamos el comando "withColumn", tal como se observa.

```
from pyspark.sql.types import *

dfVenta = dfVenta.withColumn("Precio", dfVenta["Precio"].cast(DecimalType(10,2)))
dfVenta = dfVenta.withColumn("Cantidad", dfVenta["Cantidad"].cast(IntegerType()))
dfVenta = dfVenta.withColumn("Fecha", dfVenta["Fecha"].cast(DateType()))
```

Creando VISTA por cada DataFrame, las vista son como tablas que podemos hacer INNER JOIN
```
dfCliente.createOrReplaceTempView("cliente")
dfVenta.createOrReplaceTempView("venta")
```

USANDO spark.sql, podemos hacer una consulta usando el dialecto SQL
```
spark.sql(""" 
SELECT c.Nombre_y_Apellido, SUM(v.Precio * v.Cantidad) as total
FROM venta v
INNER JOIN cliente c ON v.IdCliente = c.IdCliente
GROUP BY c.Nombre_y_Apellido
ORDER BY total DESC
LIMIT 5
           """).show(truncate=False)
```
```
+-----------------+---------+
|Nombre_y_Apellido|total    |
+-----------------+---------+
|Laura Ines Guasch|696645.52|
|Sergio Artazu    |695813.00|
|Antonio Benitez  |621303.84|
|Irma Esquivel    |546859.00|
|Sin Dato         |541618.00|
+-----------------+---------+
```

Tambien podemos usar PYSPARK, para hacer la misma consulta.
```
from pyspark.sql.functions import sum,avg,max,count

dfVenta.join(dfCliente, dfVenta.IdCliente == dfCliente.IdCliente, "inner") \
    .select(dfCliente.Nombre_y_Apellido, dfVenta.Precio, dfVenta.Cantidad) \
    .groupBy(dfCliente.Nombre_y_Apellido,) \
    .agg(sum(dfVenta.Precio * dfVenta.Cantidad).alias("total")) \
    .orderBy("total", ascending = False) \
    .show(5)
```
