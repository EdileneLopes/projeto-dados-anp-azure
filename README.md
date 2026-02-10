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



## 🚀 Diferenciais de Engenharia Aplicados
* **Parametrização:** Ingestão dinâmica utilizando arquivos JSON para controle de parâmetros no Data Factory.
* **Modularização:** Divisão do processamento em notebooks distintos para Ingestão, Transformação e Carga.
* **Segurança:** Preparado para integração com **Azure Key Vault** para gestão de credenciais JDBC.
* **Performance:** Geração de Surrogate Keys (IDs) e tipagem otimizada para SQL Server.

## 📈 Reflexão Técnica (Evolução de DBA para Engenheira)
Como profissional com background em DBA Cloud, este projeto visa transição para Engenharia de Dados. 
* **O que aprendi:** A diferença entre gerenciar o banco de dados e gerenciar o ciclo de vida do dado em movimento. Foquei em conceitos de processamento distribuído, orquestração de pipelines complexos e a importância da separação entre armazenamento e processamento.
* **Melhorias para a V2:** Implementação de tabelas no formato **Delta** para garantir transações ACID no Data Lake e uso de **Unity Catalog** para governança.
