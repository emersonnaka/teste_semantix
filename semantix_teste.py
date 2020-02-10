#!/usr/bin/env python3
# coding: utf-8

# # Teste Semantix

# 1. Qual o objetivo do comando cache em Spark?
# 
# Resposta: Manter o conjunto de dados em memória em todo o cluster
# 
# 

# 2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
# MapReduce. Por quê?
# 
# Resposta: Enquanto o Hadoop move os dados por meio dos discos e da rede, o Spark move os dados em memória, diminuindo a quantidade I/O consideravelmente.
# 
# 

# 3. Qual é a função do SparkContext?
# 
# Resposta: O SparkContext realiza a conexão com o cluster e pode ser utilizado para criar RDD's, por exemplo.

# 4. Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
# 
# Resposta: RDD é um conjunto tolerante a falhas de objetos que podem ser processados em paralelo.

# 5. GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
# 
# Resposta: Porque o reduceByKey realiza a operação dentro das partiçoões antes de enviar para o worker responsável por realizar o reduce. Enquanto o GroupByKey não realiza a operação dentro das pertições, gerando maior tráfego de dados para o worker.

# 6. Explique o que o código Scala abaixo faz.
# '''scala
# val textFile = sc.textFile("hdfs://...")
# val counts = textFile.flatMap(line => line.split(" "))
# .map(word => (word, 1))
# .reduceByKey(_ + _)
# counts.saveAsTextFile("hdfs://...")
# '''
# 
# Resposta: O código realiza uma contagem da quantidade de ocorrência de cada palavra.
# * flatMap -> obtém o cada palavra que ocorreu na linha
# * map -> para cada palavra, atribui o valor 1 referente a uma ocorrência
# * reduceByKey -> realiza a soma de cada palavra que ocorreu mais de uma vez
# E salva em um arquivo.

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lit
from pyspark.sql.functions import sum as _sum


# In[2]:


# Essa função cria as colunas no dataframe conforme descritas no PDF
# com seus respectivos valores por meio de expressão regular
def build_df(df_source):
    df_clean = df_source.withColumn(
        'host',
        regexp_replace('value', '^([^\s]+)\s+.*', r'$1')
    )
    df_clean.cache()
    df_clean = df_clean.withColumn(
        'timestamp',
        regexp_replace('value', '^[^\[]+\[([^\]]+)\].*', r'$1')
    )
    df_clean = df_clean.withColumn(
        'date',
        regexp_replace('value', '^[^\[]+\[([^:]+)\:.*', r'$1')
    )
    df_clean = df_clean.withColumn(
        'http_request',
        regexp_replace('value', '^[^\"]+\"([^\"]+)\".*', r'$1')
    )
    df_clean = df_clean.withColumn(
        'url',
        regexp_replace('value', '^[^\"]+\"\w+\s+([^\s]+)\s+.*', r'$1')
    )
    df_clean = df_clean.withColumn(
        'request_status',
        regexp_replace('value', '.*\s+(\d+)\s[0-9\-]+$', r'$1').cast('Integer')
    )
    df_clean = df_clean.withColumn(
        'total_bytes',
        regexp_replace('value', '.*\s(\d+)$', r'$1').cast('Long')
    )
    return df_clean


# In[3]:


# Cria o SparkSession para utilizarmos o conceito de dataframe do Spark
spark = SparkSession    .builder    .appName("Teste Semantix")    .getOrCreate()


# In[4]:


# Cria os dataframes a partir dos arquivos
df_source_july = spark.read.text('NASA_access_log_Jul95')
df_source_aug = spark.read.text('NASA_access_log_Aug95')


# In[5]:


# Cria os dataframes com todas as colunas descritas no PDF
df_july = build_df(df_source_july)
df_aug = build_df(df_source_aug)
df_all = df_july.union(df_aug)
df_all.cache()
df_all = df_all.filter(df_all['request_status'].isNotNull())


# 1. Número de hosts únicos

# In[6]:


df_all.select('host').distinct().count()


# 2. O total de erros 404

# In[7]:


df_404 = df_all.filter(df_all['request_status'] == 404).select(['date', 'url', 'request_status'])
df_404.cache()
print(df_404.count())


# 3. Os 5 URLs que mais causaram erro 404

# In[8]:


df_404.groupBy('url').count()    .sort('count', ascending=False)    .show(5, truncate=False)


# 4. Quantidade de erros 404 por dia
# 
# 

# In[9]:


df_404.groupBy('date').agg(_sum(lit(1))).show(100)


# 5. O total de bytes retornados

# In[10]:


df_all.agg(_sum('total_bytes')).show()

