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


# ### Nombre archivos

# In[3]:


dir_archivo = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/"
dir_complemento = 'ecommerce/curated/'
nombre_archivos = ['productos.parquet',
                   'detalle_productos.parquet',
                   'detalle_categoria.parquet']


# #### Tabla productos

# In[4]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivos[0]}'
df_productos = spark.read.format("parquet")                .load(nombre_archivo)

df_productos = df_productos.select('sku', 'escala', 'categoria_id', 'stock')
df_productos = df_productos.withColumn('stock', F.col('stock').cast(IntegerType()))


df_productos = df_productos.withColumn('escala_split', F.split(F.col('escala'), '-'))

df_productos = df_productos.withColumn('escala_min', F.col('escala_split').getItem(0))
df_productos = df_productos.withColumn('escala_min', F.trim(F.col('escala_min')).cast(IntegerType()))

df_productos = df_productos.withColumn('escala_max', F.col('escala_split').getItem(1))
df_productos = df_productos.withColumn('escala_max', F.trim(F.col('escala_max')).cast(IntegerType()))

df_productos = df_productos.select('sku', 'escala', 'categoria_id',
                                   'stock', 'escala_max', 'escala_min')


# In[5]:


# df_productos.printSchema()


# #### Tabla detalle_productos

# In[6]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivos[1]}'
df_dt_productos = spark.read.format("parquet")                    .load(nombre_archivo)

df_dt_productos = df_dt_productos.select('sku', 'marca', 'modelo', 'year')
df_dt_productos = df_dt_productos.withColumn('year', F.col('year').cast(IntegerType()))


# In[7]:


# df_dt_productos.printSchema()


# #### Tabla detalle_categoria

# In[8]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivos[2]}'
df_dt_categoria = spark.read.format("parquet")                    .load(nombre_archivo)


# In[9]:


# df_dt_categoria.printSchema()


# ### Cruce de tablas

# In[10]:


df_intermedio = df_productos.join(df_dt_productos, how='full', on=['sku'])
df_productos_expandida = df_intermedio.join(df_dt_categoria, how='full', on=['categoria_id'])
df_productos_expandida = df_productos_expandida.select(
    'sku',
    'marca',
    'modelo',
    'year',
    'escala',
    'escala_min',
    'escala_max',
    'categoria_id',
    'categoria',
    'stock')
df_productos_expandida.cache()


# In[11]:


# df_productos_expandida.printSchema()


# ### Almacenamiento

# In[12]:


nombre_destino = 'ecommerce/curated/productos_expandida.parquet'


# In[13]:


df_productos_expandida.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# #### Adicional

# In[14]:


df_filtrado = df_productos_expandida.limit(10)


# In[15]:


df_filtrado.show(3)


# In[16]:


df_pandas = df_filtrado.toPandas()
nombre_csv = "output/productos_expandida.csv"
df_pandas.to_csv(nombre_csv, index=False)


# ### Unpersist

# In[17]:


df_productos_expandida.unpersist()

