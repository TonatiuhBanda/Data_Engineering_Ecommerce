#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import *


# In[2]:


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


# ### Limpieza de acentos y saltos de línea

# In[3]:


dir_archivo = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/"


# In[4]:


nombre_archivos = [
    'detalle_categoria.parquet',
    'detalle_cliente.parquet',
    'detalle_productos.parquet',
    'detalle_sucursal.parquet',
    'ordenes.parquet',
    'productos.parquet',
    'hechos.parquet']


# In[5]:


letras_acento = 'áéíóúÁÉÍÓÚ'
letras_sin_acento = 'aeiouAEIOU'
tabla_acentos = str.maketrans(letras_acento, letras_sin_acento)

for archivo in nombre_archivos:
    nombre_archivo = f"ecommerce/stage/{archivo}"
    df = spark.read.format("parquet")            .load(dir_archivo+nombre_archivo)

    for col_nombre in df.columns:
        df = df.withColumn(col_nombre, F.translate(col_nombre,
                                                   letras_acento,
                                                   letras_sin_acento))
        df = df.withColumn(col_nombre, F.translate(col_nombre, '\n', ''))
        df = df.withColumnRenamed(col_nombre, col_nombre.translate(tabla_acentos))

    nombre_destino = f"ecommerce/curated/{archivo}"
    df.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# In[6]:


#df.show(n=3)
#df.show(n=2, vertical=True, truncate=False)

