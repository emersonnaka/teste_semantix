# Teste Semantix

1. Qual o objetivo do comando cache em Spark?

Resposta: Manter o conjunto de dados em memória em todo o cluster



2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?

Resposta: Enquanto o Hadoop move os dados por meio dos discos e da rede, o Spark move os dados em memória, diminuindo a quantidade I/O consideravelmente.



3. Qual é a função do SparkContext?

Resposta: O SparkContext realiza a conexão com o cluster e pode ser utilizado para criar RDD's, por exemplo.

4. Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

Resposta: RDD é um conjunto tolerante a falhas de objetos que podem ser processados em paralelo.

5. GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

Resposta: Porque o reduceByKey realiza a operação dentro das partiçoões antes de enviar para o worker responsável por realizar o reduce. Enquanto o GroupByKey não realiza a operação dentro das pertições, gerando maior tráfego de dados para o worker.

6. Explique o que o código Scala abaixo faz.
'''scala
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
.map(word => (word, 1))
.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
'''

Resposta: O código realiza uma contagem da quantidade de ocorrência de cada palavra.
* flatMap -> obtém o cada palavra que ocorreu na linha
* map -> para cada palavra, atribui o valor 1 referente a uma ocorrência
* reduceByKey -> realiza a soma de cada palavra que ocorreu mais de uma vez
E salva em um arquivo.


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lit
from pyspark.sql.functions import sum as _sum
```


```python
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
```


```python
# Cria o SparkSession para utilizarmos o conceito de dataframe do Spark
spark = SparkSession\
    .builder\
    .appName("Teste Semantix")\
    .getOrCreate()
```


```python
# Cria os dataframes a partir dos arquivos
df_source_july = spark.read.text('NASA_access_log_Jul95')
df_source_aug = spark.read.text('NASA_access_log_Aug95')
```


```python
# Cria os dataframes com todas as colunas descritas no PDF
df_july = build_df(df_source_july)
df_aug = build_df(df_source_aug)
df_all = df_july.union(df_aug)
df_all.cache()
df_all = df_all.filter(df_all['request_status'].isNotNull())
```

1. Número de hosts únicos


```python
df_all.select('host').distinct().count()
```




    137978



2. O total de erros 404


```python
df_404 = df_all.filter(df_all['request_status'] == 404).select(['date', 'url', 'request_status'])
df_404.cache()
print(df_404.count())
```

    20901


3. Os 5 URLs que mais causaram erro 404


```python
df_404.groupBy('url').count()\
    .sort('count', ascending=False)\
    .show(5, truncate=False)
```

    +--------------------------------------------+-----+
    |url                                         |count|
    +--------------------------------------------+-----+
    |/pub/winvn/readme.txt                       |2004 |
    |/pub/winvn/release.txt                      |1732 |
    |/shuttle/missions/STS-69/mission-STS-69.html|682  |
    |/shuttle/missions/sts-68/ksc-upclose.gif    |426  |
    |/history/apollo/a-001/a-001-patch-small.gif |384  |
    +--------------------------------------------+-----+
    only showing top 5 rows
    


4. Quantidade de erros 404 por dia




```python
df_404.groupBy('date').agg(_sum(lit(1))).show(100)
```

    +-----------+------+
    |       date|sum(1)|
    +-----------+------+
    |02/Jul/1995|   291|
    |21/Aug/1995|   305|
    |06/Aug/1995|   373|
    |16/Jul/1995|   257|
    |07/Aug/1995|   537|
    |11/Aug/1995|   263|
    |27/Jul/1995|   336|
    |07/Jul/1995|   570|
    |17/Jul/1995|   406|
    |15/Jul/1995|   254|
    |18/Jul/1995|   465|
    |26/Jul/1995|   336|
    |03/Aug/1995|   304|
    |18/Aug/1995|   256|
    |17/Aug/1995|   271|
    |14/Aug/1995|   287|
    |10/Jul/1995|   398|
    |04/Jul/1995|   359|
    |20/Aug/1995|   312|
    |20/Jul/1995|   428|
    |24/Aug/1995|   420|
    |27/Aug/1995|   370|
    |13/Aug/1995|   216|
    |15/Aug/1995|   327|
    |28/Jul/1995|    94|
    |12/Jul/1995|   471|
    |25/Aug/1995|   415|
    |06/Jul/1995|   640|
    |22/Aug/1995|   288|
    |08/Aug/1995|   391|
    |22/Jul/1995|   192|
    |01/Jul/1995|   316|
    |23/Jul/1995|   233|
    |21/Jul/1995|   334|
    |23/Aug/1995|   345|
    |19/Aug/1995|   209|
    |24/Jul/1995|   328|
    |26/Aug/1995|   366|
    |31/Aug/1995|   526|
    |28/Aug/1995|   410|
    |04/Aug/1995|   346|
    |12/Aug/1995|   196|
    |19/Jul/1995|   639|
    |30/Aug/1995|   571|
    |05/Aug/1995|   236|
    |08/Jul/1995|   302|
    |09/Jul/1995|   348|
    |01/Aug/1995|   243|
    |25/Jul/1995|   461|
    |05/Jul/1995|   497|
    |11/Jul/1995|   471|
    |29/Aug/1995|   420|
    |13/Jul/1995|   532|
    |16/Aug/1995|   259|
    |09/Aug/1995|   279|
    |10/Aug/1995|   315|
    |14/Jul/1995|   413|
    |03/Jul/1995|   474|
    +-----------+------+
    


5. O total de bytes retornados


```python
df_all.agg(_sum('total_bytes')).show()
```

    +----------------+
    |sum(total_bytes)|
    +----------------+
    |     65524314915|
    +----------------+
    

