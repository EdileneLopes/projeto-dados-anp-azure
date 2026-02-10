# projeto-dados-anp-azure
# Pipeline de Dados ANP - Ingestão e Modelagem Dimensional na Azure

Este projeto demonstra a construção de um pipeline de dados completo (End-to-End) utilizando a plataforma Azure, focado na coleta e tratamento de dados públicos da ANP (Agência Nacional do Petróleo).

## 🛠️ Tecnologias Utilizadas
* **Azure Data Factory:** Orquestração e ingestão de dados via API/HTTP.
* **Azure Data Lake Storage (Gen2):** Armazenamento em camadas (Arquitetura Medallion).
* **Azure Databricks (PySpark):** Processamento distribuído e transformação dos dados.
* **Azure SQL Database:** Destino final para consumo em ferramentas de Analytics.

## 🏗️ Arquitetura e Fluxo de Dados
O projeto segue o conceito de **Arquitetura Medallion**:

1. **Bronze (Raw):** Dados extraídos do portal gov.br via ADF e armazenados em formato CSV original.
2. **Silver (Trusted):** Limpeza de strings, normalização de tipos (decimais, datas) e tratamento de nulos via Databricks.
3. **Gold (Refined):** Modelagem dimensional (Star Schema) com criação de tabelas Fato e Dimensões.

## 📚 Créditos e Aprendizado
Este projeto foi desenvolvido como parte de um estudo prático guiado por especialistas da comunidade de dados (YouTube), servindo como laboratório para aplicação de conceitos de Engenharia de Dados em ambiente Azure. A partir da base proposta, implementei melhorias focadas em padronização de tipos e boas práticas de banco de dados.

## 🚀 Diferenciais de Engenharia Aplicados
* **Parametrização:** Ingestão dinâmica utilizando arquivos JSON para controle de parâmetros no Data Factory.
* **Modularização:** Divisão do processamento em notebooks distintos para Ingestão, Transformação e Carga.
* **Segurança:** Preparado para integração com **Azure Key Vault** para gestão de credenciais JDBC.
* **Performance:** Geração de Surrogate Keys (IDs) e tipagem otimizada para SQL Server.

## 📈 Reflexão Técnica (Evolução de DBA para Engenheira)
Como profissional com background em DBA Cloud, utilizei este projeto guiado para materializar conceitos teóricos de Engenharia de Dados. 

* **Capacidade de Execução:** Embora o fluxo tenha sido baseado em referências educacionais, a análise crítica sobre a tipagem (`Decimal` vs `Float`), a estruturação do script DDL e a escolha das Surrogate Keys foram decisões onde apliquei minha bagagem prévia em SQL Server para garantir um ambiente Gold performático.
* **O que aprendi:** A diferença entre gerenciar o dado estático e o dado em movimento (ETL/ELT). Entendi como o Spark distribui o processamento, algo bem diferente da execução de queries em um motor relacional tradicional.
