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
nombre_archivo = 'ordenes.parquet'


# #### Tabla ordenes

# In[4]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivo}'
df_ordenes = spark.read.format("parquet")                .load(nombre_archivo)

df_ordenes = df_ordenes.withColumn('precio', F.col('precio').cast(FloatType()))


# In[5]:


# df_ordenes.printSchema()


# ### Agrupación

# In[6]:


df_resumen = df_ordenes.groupby('orden_id', 'fecha_orden')                .agg(F.count('sku').alias('numero_productos'),
                     F.sum('precio').alias('total_venta'))

df_resumen = df_resumen.withColumn('numero_productos',
                                   F.col('numero_productos').cast(IntegerType()))

df_resumen = df_resumen.select('orden_id',
                               'numero_productos',
                               'total_venta',
                               'fecha_orden')
df_resumen.cache()


# In[7]:


df_resumen.show(n=3, vertical=True, truncate=False)


# In[8]:


# df_resumen.printSchema()


# ### Almacenamiento

# In[9]:


nombre_destino = 'ecommerce/curated/ordenes_resumen.parquet'


# In[10]:


df_resumen.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# #### Adicional

# In[11]:


df_filtrado = df_resumen.limit(10)


# In[12]:


df_filtrado.show(n=2, vertical=True, truncate=False)


# In[13]:


df_pandas = df_filtrado.toPandas()
nombre_csv = "output/ordenes_resumen.csv"
df_pandas.to_csv(nombre_csv, index=False)


# ### Unpersist

# In[14]:


df_resumen.unpersist()

