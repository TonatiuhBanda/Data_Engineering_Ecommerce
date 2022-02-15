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


# ### Tabla detalle cliente

# In[3]:


dir_archivo = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/"
nombre_archivo = 'ecommerce/curated/detalle_cliente.parquet'


# In[4]:


df = spark.read.format("parquet")        .load(dir_archivo+nombre_archivo)


# In[5]:


df.select('direccion').show(n=10, truncate=False)


# #### Direccion

# In[6]:


df_procesado = df.withColumn('direccion_split', F.split(F.col('direccion'), ','))
df_procesado = df_procesado.withColumn('numero_elementos', F.size(F.col('direccion_split')))

df_procesado = df_procesado.withColumn('estado_cp', F.col('direccion_split').getItem(F.col('numero_elementos') - 1 ))
df_procesado = df_procesado.withColumn('estado_cp', F.trim(F.col('estado_cp')))

df_procesado = df_procesado.withColumn('estado_cp_split', F.split(F.col('estado_cp'), ' '))
df_procesado = df_procesado.withColumn('numero_elementos_estado', F.size(F.col('estado_cp_split')))
df_procesado = df_procesado.withColumn('codigo_postal', F.col('estado_cp_split').getItem(F.col('numero_elementos_estado') - 1))

df_procesado = df_procesado.withColumn('estado_cp_split_drop', F.expr("slice(estado_cp_split, 1, numero_elementos_estado-1)"))
df_procesado = df_procesado.withColumn('estado', F.concat_ws(' ', 'estado_cp_split_drop'))

df_procesado = df_procesado.withColumn('direccion_split_drop', F.expr("slice(direccion_split, 1, numero_elementos-1)"))
df_procesado = df_procesado.withColumn('direccion_procesado', F.concat_ws(' ', 'direccion_split_drop'))


# In[7]:


df_direccion = df_procesado.drop(
    'direccion_split',
    'numero_elementos',
    'estado_cp',
    'estado_cp_split',
    'numero_elementos_estado',
    'estado_cp_split_drop',
    'direccion_split_drop',
    'direccion')

df_direccion = df_direccion.withColumnRenamed('direccion_procesado', 'direccion')
df_direccion.cache()


# In[8]:


df_direccion.show(n=5, vertical=True, truncate=False)


# ### Almacenamiento

# In[9]:


nombre_destino = 'ecommerce/curated/detalle_cliente_direccion.parquet'


# In[10]:


df_direccion.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# #### Adicional

# In[11]:


df_filtrado = df_direccion.limit(10)


# In[12]:


df_filtrado.show(3)


# In[13]:


df_pandas = df_filtrado.toPandas()
nombre_csv = "output/detalle_cliente_direccion.csv"
df_pandas.to_csv(nombre_csv, index=False)


# ### Unpersist

# In[14]:


df_direccion.unpersist()

