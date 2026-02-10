# Notebook 02: Limpeza e Padronização (Camada Silver)
# Objetivo: Padronizar nomes, tratar tipos de dados e preparar para a modelagem.

from pyspark.sql.functions import col, regexp_replace, to_date

# 1. Recuperando dados da Camada Bronze
df_bronze = spark.table("TABELA_ANP_BRONZE")

# 2. Renomeação de Colunas
# Padronizando para letras maiúsculas e sem caracteres especiais (Snake Case)
df_rename = df_bronze.withColumnRenamed("Regiao - Sigla", "REGIAO") \
                     .withColumnRenamed("Estado - Sigla", "ESTADO") \
                     .withColumnRenamed("Municipio", "MUNICIPIO") \
                     .withColumnRenamed("Revenda", "REVENDA") \
                     .withColumnRenamed("CNPJ da Revenda", "CNPJ") \
                     .withColumnRenamed("Nome da Rua", "RUA") \
                     .withColumnRenamed("Numero Rua", "NUMERO") \
                     .withColumnRenamed("Complemento", "COMPLEMENTO") \
                     .withColumnRenamed("Bairro", "BAIRRO") \
                     .withColumnRenamed("Cep", "CEP") \
                     .withColumnRenamed("Produto", "PRODUTO") \
                     .withColumnRenamed("Data da Coleta", "DT_COLETA_RAW") \
                     .withColumnRenamed("Valor de Venda", "VL_VENDA_RAW") \
                     .withColumnRenamed("Valor de Compra", "VL_COMPRA_RAW") \
                     .withColumnRenamed("Unidade de Medida", "UNID_MEDIDA") \
                     .withColumnRenamed("Bandeira", "BANDEIRA")

# 3. Tratamento de Tipos e Conversão Numérica
# Nota: Como DBA, prezei pela precisão decimal(18,3) para evitar erros de arredondamento.
df_silver = df_rename.withColumn("DT_COLETA", to_date(col("DT_COLETA_RAW"), "dd/MM/yyyy")) \
                     .withColumn("VL_VENDA", regexp_replace(col("VL_VENDA_RAW"), ",", ".").cast("decimal(18,3)")) \
                     .withColumn("VL_COMPRA", regexp_replace(col("VL_COMPRA_RAW"), ",", ".").cast("decimal(18,3)")) \
                     .drop("DT_COLETA_RAW", "VL_VENDA_RAW", "VL_COMPRA_RAW")

# 4. Persistência na Camada Silver
df_silver.createOrReplaceTempView("TABELA_ANP_SILVER")

print(f"Camada Silver processada. Esquema de dados validado.")
