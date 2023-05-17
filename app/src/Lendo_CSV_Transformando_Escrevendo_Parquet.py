#Importantando pacotes da Biblioteca pyspark

import pyspark 
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

#Configurações que serão utilizadas para a execução (passível de mudança caso necessária)

spark  = SparkSession.builder\
	        .master("local")\
        .appName("cadastro_positivo")\
        .config("spark.shuffle.service.enabled", "true")\
        .config("spark.dynamicAllocation.enabled", "true")\
        .config("spark.dynamicAllocation.initialExecutors", "1")\
        .config("spark.dynamicAllocation.minExecutors", "1")\
        .config("spark.dynamicAllocation.maxExecutors" "10 (20)")\
        .config("spark.executor.cores", "2")\
        .config("spark.executor.memory", "5G")\
        .config("spark.driver.cores", "4")\
        .config("spark.driver.memory", "4G")\
        .config("spark.yarn.executor.memoryOverhead", "600")\
        .config("spark.yarn.driver.memoryOverhead", "600")\
        .config("spark.ui.port", "4142")\
        .config("spark.ui.enabled", "false")\
        .config("spark.shuffle.compress", "true")\
        .config("spark.driver.maxResultSize", "900m")\
        .config("spark.default.parallelism", "5000")\
        .config("spark.executor.heartbeatInterval", "10s")\
        .config("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "1s")\
        .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "120s")\
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s")\
        .config("spark.sql.broadcastTimeout", "36000")\
        .config("spark.network.timeout", "600s")\
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .config("spark.sql.shuffle.partitions", "5000")\
        .enableHiveSupport()\
        .getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

#LENDO NOSSO ARQUIVO PARQUET

df = spark.read.option("header","true").option("delimiter",",").csv("./../../sources/tb_salarios.csv")
print("Printando o dataframe!")
df.show()

#CRIANDO A TABELA TEMPORÁRIA PARA USAR O SPARK SQL

df.createOrReplaceTempView("tbl_cadastro")
print("Printando a tabela temorária")
print("tbl_cadastro")

#MOSTRAR COM UM SELECT E ARMAZENAR O RESULTADO EM UMA VARIAVEL (DF)

#spark.sql("DESCRIBE tbl_cadastro ").show()
resp = spark.sql("SELECT COUNT(*) AS Count_Data_Engineer FROM tbl_cadastro WHERE job_title='Data Engineer'")
print("Mostrando o resultado da query")
resp.show()

#ESCREVER O DF EM UM ARQUIVO

resp.write.format("parquet").option("header","true").option("delimiter",";").mode("append").save("./../../sink/Lendo_CSV_Transformando_Escrevendo_Parquet")
print("Mostrando o arquivo gravado com sucesso")









