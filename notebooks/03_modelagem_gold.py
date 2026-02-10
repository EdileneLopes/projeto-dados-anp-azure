# Notebook 03: Modelagem Dimensional (Camada Gold) e Carga
# Objetivo: Transformar dados normalizados em Star Schema (Fato e Dimensões) e persistir no DW.

from pyspark.sql.functions import monotonically_increasing_id, col

# 1. Funções de Suporte (Boas Práticas de Reuso)
def carga_sql_server(df, tabela):
    """Realiza a carga JDBC para o Azure SQL Database."""
    # Nota: Credenciais gerenciadas via Secret Scope para segurança 
    jdbc_user = dbutils.secrets.get(scope="anp_scope", key="username")
    jdbc_password = dbutils.secrets.get(scope="anp_scope", key="password")
    
    df.write.format("jdbc") \
        .option("url", "jdbc:sqlserver://bdtreino2.database.windows.net:1433") \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .option("database", "bdteste") \
        .option("dbtable", tabela) \
        .mode("overwrite") \
        .save()

# 2. Criação das Tabelas de Dimensão
# Para cada dimensão, extraídos valores distintos e gerados a Surrogate Key (ID)
df_silver = spark.table("TABELA_ANP_SILVER")

# Exemplo: Dimensão Revenda
df_revenda = df_silver.select("REVENDA", "CNPJ").distinct().orderBy("CNPJ")
df_revenda_id = df_revenda.withColumn("ID_REVENDA", monotonically_increasing_id() + 1)
carga_sql_server(df_revenda_id, "DW.DIM_REVENDA")

# Exemplo: Dimensão Produto
df_produto = df_silver.select("PRODUTO").distinct().where("PRODUTO IS NOT NULL")
df_produto_id = df_produto.withColumn("ID_PRODUTO", monotonically_increasing_id() + 1)
carga_sql_server(df_produto_id, "DW.DIM_PRODUTO")

# [Nota: O processo se repete para as demais dimensões: ESTADO, MUNICIPIO, REGIAO, etc.]

# 3. Construção da Tabela Fato
# Utilizados Joins para substituir os atributos descritivos pelos seus respectivos IDs (FKs)
df_fato = df_silver.alias("S") \
    .join(df_revenda_id.alias("R"), col("S.CNPJ") == col("R.CNPJ"), "left") \
    .join(df_produto_id.alias("P"), col("S.PRODUTO") == col("P.PRODUTO"), "left") \
    # ... adicione os outros joins aqui seguindo o mesmo padrão ...

df_fato_final = df_fato.select(
    "S.DT_COLETA",
    "S.VL_VENDA",
    "S.VL_COMPRA",
    "R.ID_REVENDA",
    "P.ID_PRODUTO"
    # ... selecione os IDs das outras dimensões ...
).limit(2000) # Limite aplicado para fins de demonstração no ambiente de lab

# 4. Persistência Final (Multi-Destino, arquitetura hibrida)

# Destino A: Data Lake (Formato Delta - Padrão Lakehouse ou blob storage)
#a intenção é não sobrecarregar o banco com consultas
df_fato_final.write.format("delta").mode("overwrite").save("/mnt/engdados/refined/fato_anp")

# Destino B: Data Warehouse (Azure SQL Database - destino final o banco de dados)
carga_sql_server(df_fato_final, "DW.FATO_ANP")

print("Pipeline Gold finalizado com sucesso. Tabelas Fato e Dimensões persistidas.")
