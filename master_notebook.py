# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 1- ANÁLISE DE CONFLITOS SOCIAIS NOS ESTADOS BRASILEIROS
# MAGIC
# MAGIC O objetivo desse Notebook é analisar um conjunto de dados de conflitos pacíficos e violentos no Brasil, a fim de responder algumas perguntas:
# MAGIC
# MAGIC 1- Quais as TOP 10 cidades mais Violentos do Brasil?
# MAGIC
# MAGIC 2- Que tipo de conflitos são mais comuns nessas cidades ?
# MAGIC
# MAGIC 1- Qual a evolução dos conflitos ao longo dos anos?
# MAGIC
# MAGIC 2- Como estão distribuídos geograficamente?
# MAGIC
# MAGIC 3- Que tipo de conflitos são mais comuns?
# MAGIC
# MAGIC 4- Qual a relação entre os iniciadores e conflitantes?
# MAGIC
# MAGIC 5- Qual a realação entre os conflitos e indicadores demográficos?
# MAGIC
# MAGIC
# MAGIC Coleta de dados;
# MAGIC Ingestão de dados;
# MAGIC Processamento de dados;
# MAGIC Transformação de dados;
# MAGIC Análise de dados;
# MAGIC Visualização de dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2- COLETA DE DADOS 
# MAGIC
# MAGIC 1- Download dos arquivos CSV do kaggle (FileStore/tables)
# MAGIC
# MAGIC CONFLITO (Brazil Political Violence and Protests Dataset.csv)
# MAGIC
# MAGIC https://www.kaggle.com/datasets/justin2028/brazil-conflict-tracker-20182023/data
# MAGIC
# MAGIC CIDADE (BRAZIL_CITIES_REV2022.csv)
# MAGIC
# MAGIC https://www.kaggle.com/datasets/crisparada/brazilian-cities

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - INGESTÃO DE DADOS - ROW (CAMADA BRONZE)
# MAGIC
# MAGIC 1- Criação do DATABASE Bronze
# MAGIC
# MAGIC 2- Carga de dados dos CONFLITOS no Brasil.
# MAGIC
# MAGIC 3- Carga de dados das CIDADES Brasileiras.
# MAGIC

# COMMAND ----------

# DBTITLE 1,1- Criação do DATABASE Bronze
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze

# COMMAND ----------

# DBTITLE 1,2- Carga de dados dos CONFLITOS no Brasil.
# 2 - Carga de dados dos CONFLITOS no Brasil.

# File location and type
file_location = "/FileStore/tables/Brazil_Political_Violence_and_Protests_Dataset.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_conflito = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_conflito)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze.conflito

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/bronze.db/conflito',True)

df_conflito.write.format("delta").mode("append").saveAsTable("bronze.conflito")

# COMMAND ----------

# DBTITLE 1,3- Carga de dados das CIDADES Brasileiras.
# 3- Carga de dados das CIDADES Brasileiras. 

# File location and type
file_location = "/FileStore/tables/BRAZIL_CITIES_REV2022.CSV"
file_type = "CSV"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_cidade = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_cidade)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze.cidade

# COMMAND ----------

## primeiro tratamento de dados: colunas com espaço não são aceitas para criar tabela no DATABASE
df_cidade = df_cidade.withColumnRenamed('IDHM Ranking 2010', 'IDHM_Ranking_2010')

dbutils.fs.rm('dbfs:/user/hive/warehouse/bronze.db/cidade',True)

df_cidade.write.format("delta").mode("append").saveAsTable("bronze.cidade")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 - TRANSFORMAÇÃO DOS DADOS (SILVER)
# MAGIC
# MAGIC 1- Tratamento do campo LOCATION na tabela Conflito - Removento acentos
# MAGIC
# MAGIC 2- Tratamento do campo EVENT_DATE na tabela Conflito - Transformando em mês e ano
# MAGIC
# MAGIC 3- Tratamento do campo CITY na tabela Cidades - Removento acentos
# MAGIC
# MAGIC 4- Limpeza e transformação dos dados: tradução, eliminação de colunas, rename de colunas
# MAGIC
# MAGIC 5- Criação do DATABASE Silver e Carga das tabelas Conflito e Cidade

# COMMAND ----------

# MAGIC %sql
# MAGIC select upper(LOCATION), count(*) from bronze.conflito where UPPER(LOCATION) like '%ZONE%' group by LOCATION order by LOCATION 

# COMMAND ----------

# DBTITLE 1,1- Tratamento do campo LOCATION na tabela Conflito - Removento acentos
## carregando os dados do DATABASE bronze para tratamento dos dados de Conflito

df_conflito_bronze_sql = spark.sql('''select * from bronze.conflito''')

# COMMAND ----------

##  1- Tratamento do campo LOCATION na tabela Conflito - Removento acentos

from pyspark.sql.functions import translate, upper
from pyspark.sql.functions import *

acento = 'áàãâéèêíìóòôõúùû'
sem_acento = 'aaaaeeeiioooouuu'
df_conflito2 = df_conflito_bronze_sql.withColumn('LOCATION_2', upper(translate(df_conflito_bronze_sql['LOCATION'], acento, sem_acento)))

df_conflito3 = df_conflito2.withColumn('LOCATION_3', when(col('LOCATION_2').like ('%RIO DE JANEIRO%'), 'RIO DE JANEIRO').otherwise(col('LOCATION_2')))

df_conflito4 = df_conflito3.withColumn('ID_CITY', when(col('LOCATION_3').like ('%SAO PAULO%'), 'SAO PAULO').otherwise(col('LOCATION_3')))

df_conflito4.display()


# COMMAND ----------

# DBTITLE 1,2- Tratamento do campo EVENT_DATE na tabela Conflito - Transformando em mês e ano
# 2- Tratamento do campo EVENT_DATE na tabela Conflito - Transformando em mês e ano

from pyspark.sql.functions import *

df_conflito4 = df_conflito4.withColumn('EVENT_MONTH', to_date(col('EVENT_DATE'),"dd-MM-yyyy"))
df_conflito4 = df_conflito4.withColumn('EVENT_YEAR', to_date(col('EVENT_DATE'),"dd-MM-yyyy"))
display(df_conflito4)


# COMMAND ----------

## carregando os dados do DATABASE bronze para tratamento dos dados de Cidade

df_cidade_bronze_sql = spark.sql('''select * from bronze.cidade''')

# COMMAND ----------

# DBTITLE 1,3- Tratamento do campo CITY na tabela Cidades - Removento acentos
#  3- Tratamento do campo CITY na tabela Cidade - Removento acentos

from pyspark.sql.functions import translate, upper
from pyspark.sql.functions import *

acento = 'áàãâéèêíìóòôõúùû'
sem_acento = 'aaaaeeeiioooouuu'
df_cidade2 = df_cidade_bronze_sql.withColumn('ID_CITY', upper(translate(df_cidade_bronze_sql['CITY'], acento, sem_acento)))

print(df_cidade2.columns)
print(df_conflito4.columns)

# COMMAND ----------

# DBTITLE 1,4- Limpeza e transformação dos dados: tradução, eliminação de colunas, rename de colunas
# 4 - Limpeza e transformação dos dados: tradução, eliminação de colunas, rename de colunas
from pyspark.sql.types import IntegerType

lista_conflito = ['EVENT_DATE','LOCATION','LATITUDE','LONGITUDE','LOCATION_2','LOCATION_3']
df_conflito4 = df_conflito4.drop(*lista_conflito)

df_conflito4 = df_conflito4\
  .withColumnRenamed('EVENT_TYPE', 'TIPO_EVENTO')\
  .withColumnRenamed('SUB_EVENT_TYPE', 'SUB_TIPO_EVENTO')\
  .withColumnRenamed('ACTOR1', 'ATOR_PRIMARIO')\
  .withColumnRenamed('ACTOR2', 'ATOR_SECUNDARIO')\
  .withColumnRenamed('COUNTRY', 'PAIS')\
  .withColumnRenamed('SOURCE_SCALE', 'ESCALA_GEOGRAFICA')\
  .withColumnRenamed('NOTES', 'DESCRICAO')\
  .withColumnRenamed('FATALITIES', 'FATALIDADE')\
  .withColumnRenamed('EVENT_MONTH', 'MES')\
  .withColumnRenamed('EVENT_YEAR', 'ANO')

df_conflito4 = df_conflito4.withColumn('FATALIDADE', df_conflito4['FATALIDADE'].cast(IntegerType()))

print(df_conflito4.columns)

lista_cidade = ['CITY', 'IBGE_RES_POP', 'IBGE_RES_POP_BRAS', 'IBGE_RES_POP_ESTR', 'IBGE_DU', 'IBGE_DU_URBAN', 'IBGE_DU_RURAL', 'IBGE_POP', 'IBGE_1', 'IBGE_1-4', 'IBGE_5-9', 'IBGE_10-14', 'IBGE_15-59', 'IBGE_60+', 'IBGE_PLANTED_AREA', 'IBGE_CROP_PRODUCTION_$', 'IDHM_Ranking_2010', 'IDHM', 'IDHM_Renda', 'IDHM_Longevidade', 'IDHM_Educacao', 'LONG', 'LAT', 'ALT', 'PAY_TV', 'FIXED_PHONES', 'AREA', 'REGIAO_TUR', 'CATEGORIA_TUR', 'ESTIMATED_POP', 'RURAL_URBAN', 'GVA_AGROPEC', 'GVA_INDUSTRY', 'GVA_SERVICES', 'GVA_PUBLIC', 'GVA_TOTAL', 'TAXES', 'GDP', 'POP_GDP', 'GDP_CAPITA', 'GVA_MAIN', 'MUN_EXPENDIT', 'COMP_TOT', 'COMP_A', 'COMP_B', 'COMP_C', 'COMP_D', 'COMP_E', 'COMP_F', 'COMP_G', 'COMP_H', 'COMP_I', 'COMP_J', 'COMP_K', 'COMP_L', 'COMP_M', 'COMP_N', 'COMP_O', 'COMP_P', 'COMP_Q', 'COMP_R', 'COMP_S', 'COMP_T', 'COMP_U', 'HOTELS', 'BEDS', 'Pr_Agencies', 'Pu_Agencies', 'Pr_Bank', 'Pu_Bank', 'Pr_Assets', 'Pu_Assets', 'Cars', 'Motorcycles', 'Wheeled_tractor', 'UBER', 'MAC', 'WAL-MART', 'POST_OFFICES']
df_cidade2 = df_cidade2.drop(*lista_cidade)

df_cidade2 = df_cidade2\
  .withColumnRenamed('STATE', 'ESTADO')\

print(df_cidade2.columns)



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.conflito

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.cidade

# COMMAND ----------

# DBTITLE 1,5- Criação do DATABASE Silver e Carga das tabelas Conflito e Cidade
# 5- Criação do DATABASE Silver e Carga das tabelas Conflito e Cidade

dbutils.fs.rm('dbfs:/user/hive/warehouse/silver.db/conflito',True)

df_conflito4.write.format("delta").mode("append").saveAsTable("silver.conflito")

dbutils.fs.rm('dbfs:/user/hive/warehouse/silver.db/cidade',True)

df_cidade2.write.format("delta").mode("append").saveAsTable("silver.cidade")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 - JUNTANDO INFORMAÇÕES DE CONFLITOS E CIDADES (GOLD)
# MAGIC
# MAGIC 1- Dados tratados para uso de processamento analítico ou BI
# MAGIC
# MAGIC 2- Criação do DATABASE gold e Carga das tabela Conflito
# MAGIC
# MAGIC 3- Descrição do catálogo de dados 

# COMMAND ----------

# DBTITLE 1,1- Dados tratados para uso de processamento analítico ou BI
## carregando os dados do DATABASE silver para tratamento dos dados de Conflito e Cidade

df_conflito_silver_sql = spark.sql('''select cidade.ESTADO, conflito.* from silver.conflito, silver.cidade where conflito.ID_CITY = cidade.ID_CITY''')

display(df_conflito_silver_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold.conflito

# COMMAND ----------

# DBTITLE 1,2- Criação do DATABASE gold e Carga das tabela Conflito
# 2- Criação do DATABASE gold e Carga das tabela Conflito

dbutils.fs.rm('dbfs:/user/hive/warehouse/gold.db/conflito',True)

df_conflito_silver_sql.write.format("delta").mode("append").saveAsTable("gold.conflito")

# COMMAND ----------

# DBTITLE 1,3- Descrição do catálogo de dados
spark.sql('DESCRIBE gold.conflito').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ['ESTADO', 'TIPO_EVENTO', 'SUB_TIPO_EVENTO', 'ATOR_PRIMARIO', 'ATOR_SECUNDARIO', 'PAIS', 'ESCALA_GEOGRAFICA', 'DESCRICAO', 'FATALIDADE', 'ID_CITY', 'MES', 'ANO']

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 - ANALISE DOS DADOS - SQL DASHBOARD (DATAWAREHOUSE)
# MAGIC
# MAGIC 1 - Quais as TOP 10 cidades mais Violentos do Brasil?
# MAGIC
# MAGIC 2- Que tipo de conflitos são mais comuns nessas cidades?

# COMMAND ----------

# DBTITLE 1,1 - Quais as TOP 10 cidades mais Violentos do Brasil?
## carregando os dados do DATABASE Gold para analise de dados

df_conflito_gold_sql = spark.sql('''select conflito.ID_CITY, count(*) as QTD from gold.conflito
where conflito.SUB_TIPO_EVENTO <> 'Peaceful protest'
group by conflito.ID_CITY order by 2 desc
LIMIT 10''')

lista_cidade_top_10 = df_conflito_gold_sql.agg(collect_list(col('ID_CITY'))).collect()[0][0]

display(df_conflito_gold_sql)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

#labels = df_conflito_gold_sql.select('ID_CITY').collect()[0][0]
labels = df_conflito_gold_sql.agg(collect_list(col('ID_CITY'))).collect()[0][0]
vals = df_conflito_gold_sql.agg(collect_list(col('QTD'))).collect()[0][0]
explode = (0.1,0,0,0,0,0,0,0,0,0)
fig, ax = plt.subplots(figsize=(22,15))
ax.pie(vals, labels=labels, autopct='%.2f%%')
ax.set_title('Quais as TOP 10 cidades mais Violentos do Brasil?', fontsize='16')

# COMMAND ----------

# DBTITLE 1,2 - Que tipo de conflitos são mais comuns nessas cidades?
df_conflito_gold_sql = spark.sql('''select conflito.ID_CITY, conflito.TIPO_EVENTO, conflito.SUB_TIPO_EVENTO, count(*) 
from gold.conflito
group by conflito.ID_CITY, conflito.TIPO_EVENTO, conflito.SUB_TIPO_EVENTO order by 1''')

display(df_conflito_gold_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select cidade.ID_CITY, conflito.TIPO_EVENTO, conflito.SUB_TIPO_EVENTO, count(*) from silver.conflito, silver.cidade
# MAGIC where cidade.ID_CITY = conflito.ID_CITY 
# MAGIC group by cidade.ID_CITY, conflito.TIPO_EVENTO, conflito.SUB_TIPO_EVENTO order by 1
