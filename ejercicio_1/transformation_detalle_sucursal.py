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


# ### Tabla detalle sucursal

# In[3]:


dir_archivo = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/"
nombre_archivo = 'ecommerce/curated/detalle_sucursal.parquet'


# In[4]:


df = spark.read.format("parquet")        .load(dir_archivo+nombre_archivo)


# In[5]:


df.select('sucursal').show(n=5, truncate=False)


# #### Direccion

# In[6]:


df_procesado = df.withColumn('sucursal_split', F.split(F.col('sucursal'), '-'))
df_procesado = df_procesado.withColumn('estado', F.col('sucursal_split').getItem(0))


# In[7]:


df_estado = df_procesado.drop('sucursal_split')
df_estado.cache()


# In[8]:


df_estado.show(n=5, vertical=True, truncate=False)


# ### Almacenamiento

# In[9]:


nombre_destino = 'ecommerce/curated/detalle_sucursal_estado.parquet'


# In[10]:


df_estado.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# #### Adicional

# In[11]:


df_filtrado = df_estado.limit(10)


# In[12]:


df_filtrado.show(3)


# In[13]:


df_pandas = df_filtrado.toPandas()
nombre_csv = "output/detalle_sucursal_estado.csv"
df_pandas.to_csv(nombre_csv, index=False)


# ### Unpersist

# In[14]:


df_estado.unpersist()

