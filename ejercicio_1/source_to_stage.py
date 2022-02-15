#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


dir_archivo = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/datasets/"


# In[3]:


nombre_archivos = [
    'detalle_categoria.csv',
    'detalle_cliente.csv',
    'detalle_productos.csv',
    'detalle_sucursal.csv',
    'ordenes.csv',
    'productos.csv',
    'hechos/hechos.csv']

nombre_archivo = nombre_archivos[0]


# ### Tablas

# In[4]:


for item in range(len(nombre_archivos)-1):
    nombre_archivo = nombre_archivos[item]
    df = pd.read_csv(dir_archivo+f'ecommerce/{nombre_archivo}', delimiter='|')
    
    dir_destino = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/"
    nombre_destino = f"ecommerce/stage/{nombre_archivo.replace('.csv','')}.parquet"
    
    df.to_parquet(dir_destino+nombre_destino)


# ### Tabla hechos

# In[5]:


nombre_archivo = nombre_archivos[6]
df = pd.read_csv(dir_archivo+f'ecommerce/{nombre_archivo}', delimiter='|')
df = df.rename(columns={'categoria ':'categoria'})


# In[6]:


df.head(3)


# In[7]:


dir_destino = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/"

if nombre_archivo == 'hechos/hechos.csv':
    nombre_archivo = nombre_archivo[7:]

nombre_destino = f"ecommerce/stage/{nombre_archivo.replace('.csv','')}.parquet"

df.to_parquet(dir_destino+nombre_destino)

