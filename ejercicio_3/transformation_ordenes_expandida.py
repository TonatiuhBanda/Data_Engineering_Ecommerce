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
nombre_archivos = ['detalle_categoria.parquet',
                   'detalle_cliente_direccion.parquet',
                   'detalle_productos.parquet',
                   'detalle_sucursal_estado.parquet',
                   'ordenes.parquet',
                   'productos.parquet']


# #### Tabla ordenes

# In[4]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivos[4]}'
df_ordenes = spark.read.format("parquet")                .load(nombre_archivo)

df_ordenes = df_ordenes.withColumn('cantidad', F.col('cantidad').cast(IntegerType()))
df_ordenes = df_ordenes.withColumn('precio', F.col('precio').cast(FloatType()))


# In[5]:


# df_ordenes.printSchema()


# #### Tabla detalle_sucursal

# In[6]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivos[3]}'
df_dt_sucursal = spark.read.format("parquet")                .load(nombre_archivo)

df_dt_sucursal = df_dt_sucursal.withColumnRenamed('direccion', 'sucursal_direccion')
df_dt_sucursal = df_dt_sucursal.withColumnRenamed('estado', 'sucursal_estado')
df_dt_sucursal = df_dt_sucursal.withColumnRenamed('telefono', 'sucursal_telefono')


# In[7]:


# df_dt_sucursal.printSchema()


# #### Tabla detalle_cliente

# In[8]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivos[1]}'
df_dt_cliente = spark.read.format("parquet")                .load(nombre_archivo)

df_dt_cliente = df_dt_cliente.withColumnRenamed('direccion', 'cliente_direccion')
df_dt_cliente = df_dt_cliente.withColumnRenamed('estado', 'cliente_estado')
df_dt_cliente = df_dt_cliente.withColumnRenamed('codigo_postal', 'cliente_cp')
df_dt_cliente = df_dt_cliente.withColumnRenamed('telefono', 'cliente_telefono')


# In[9]:


# df_dt_cliente.printSchema()


# #### Tabla productos

# In[10]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivos[5]}'
df_productos = spark.read.format("parquet")                .load(nombre_archivo)

df_productos = df_productos.withColumn('stock', F.col('stock').cast(IntegerType()))
df_productos = df_productos.withColumn('precio_escala', F.col('precio').cast(FloatType()))

df_productos = df_productos.withColumn('escala_split', F.split(F.col('escala'), '-'))
df_productos = df_productos.withColumn('escala_min', F.col('escala_split').getItem(0))
df_productos = df_productos.withColumn('escala_min', F.trim(F.col('escala_min')).cast(IntegerType()))
df_productos = df_productos.withColumn('escala_max', F.col('escala_split').getItem(1))
df_productos = df_productos.withColumn('escala_max', F.trim(F.col('escala_max')).cast(IntegerType()))

df_productos = df_productos.select('sku',
                                   'nombre',
                                   'escala',
                                   'escala_min',
                                   'escala_max',
                                   'precio_escala',
                                   'categoria_id',
                                   'stock')


# In[11]:


# df_productos.printSchema()


# #### Tabla detalle_productos

# In[12]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivos[2]}'
df_dt_productos = spark.read.format("parquet")                    .load(nombre_archivo)

df_dt_productos = df_dt_productos.withColumn('year', F.col('year').cast(IntegerType()))


# In[13]:


# df_dt_productos.printSchema()


# #### Tabla detalle_categoria

# In[14]:


nombre_archivo = f'{dir_archivo}{dir_complemento}{nombre_archivos[0]}'
df_dt_categoria = spark.read.format("parquet")                    .load(nombre_archivo)


# In[15]:


# df_dt_categoria.printSchema()


# ### Cruce de tablas

# In[16]:


df_od_dts = df_ordenes.join(df_dt_sucursal, how='left', on=['sucursal_id'])
df_od_dtc = df_od_dts.join(df_dt_cliente, how='left', on=['cliente_id'])
df_od_pd = df_od_dtc.join(df_productos, how='left', on=['sku'])
df_od_dtp = df_od_pd.join(df_dt_productos, how='left', on=['sku'])
df_ordenes_expandida = df_od_dtp.join(df_dt_categoria, how='left', on=['categoria_id'])
df_ordenes_expandida = df_ordenes_expandida.select(
    'orden_id',
    'sucursal_id',
    'cliente_id',
    'sku',
    'cantidad',
    'precio',
    'fecha_orden',
    'fecha_envio',
    'fecha_entrega',
    'sucursal',
    'sucursal_direccion',
    'sucursal_estado',
    'sucursal_telefono',
    'cliente',
    'cliente_direccion',
    'cliente_estado',
    'cliente_cp',
    'email',
    'cliente_telefono',
    'metodo_pago',
    'numero_tarjeta',
    'nombre',
    'escala',
    'escala_min',
    'escala_max',
    'precio_escala',
    'categoria_id',
    'stock',
    'marca',
    'modelo',
    'year',
    'categoria')
df_ordenes_expandida.cache()


# In[17]:


# df_ordenes_expandida.printSchema()


# In[18]:


df_ordenes_expandida.show(n=2, vertical=True, truncate=False)


# ### Almacenamiento

# In[19]:


nombre_destino = 'ecommerce/curated/ordenes_expandida.parquet'


# In[20]:


df_ordenes_expandida.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# #### Adicional

# In[21]:


df_filtrado = df_ordenes_expandida.limit(10)


# In[22]:


df_filtrado.show(n=2, vertical=True, truncate=False)


# In[23]:


df_pandas = df_filtrado.toPandas()
nombre_csv = "output/ordenes_expandida.csv"
df_pandas.to_csv(nombre_csv, index=False)


# ### Unpersist

# In[24]:


df_ordenes_expandida.unpersist()

