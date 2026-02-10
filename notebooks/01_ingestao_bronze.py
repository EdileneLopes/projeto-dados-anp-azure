# Notebook 01: Ingestão de Dados (Camada Bronze)
# Objetivo: Coletar e consolidar arquivos semestrais da ANP em um repositório único.

# 1. Configuração de Acesso (Mount Point)
# Nota: Em ambiente produtivo, as chaves de acesso são gerenciadas via Azure Key Vault e Secret Scopes.
dbutils.fs.mount(
  source = "wasbs://engdados@storagedadosedi.blob.core.windows.net",
  mount_point = "/mnt/engdados",
  extra_configs = {"fs.azure.account.key.storagedadosedi.blob.core.windows.net": "<CHAVE>"}
)

# 2. Ingestão Unificada com Wildcards (*)
# Em vez de múltiplas leituras e unions manuais, utilizado o suporte do Spark 
# para ler diretórios inteiros, garantindo escalabilidade para novos semestres.
caminhos_anp = [
    "/mnt/engdados/raw/2020/*.csv",
    "/mnt/engdados/raw/2021/*.csv"
]

df_bronze = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ";") \
    .load(caminhos_anp)

# 3. Persistência em Camada Bronze
# Criado uma Temporary View para que os próximos notebooks possam consumir os dados brutos.
df_bronze.createOrReplaceTempView("TABELA_ANP_BRONZE")

print(f"Ingestão concluída com sucesso. Total de registros: {df_bronze.count()}")
