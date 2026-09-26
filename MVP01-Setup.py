# Databricks notebook source
# MAGIC %md
# MAGIC ## Etapa 1 — Criação do Catálogo de Dados
# MAGIC
# MAGIC No Databricks, um **catálogo** (catalog) é o nível mais alto da hierarquia do Unity Catalog, funcionando como um contêiner lógico que organiza schemas (bancos de dados) e tabelas. Abaixo é criado o catálogo `mvpDataEngineer` que será utilizado em todo o projeto.

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS mvpDataEngineer CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cria o catálogo 'mvpDataEngineer' no Unity Catalog.
# MAGIC -- Este catálogo conterá todos os schemas e tabelas do projeto.
# MAGIC CREATE CATALOG mvpDataEngineer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 2 — Seleção do Catálogo de Trabalho
# MAGIC
# MAGIC Após criar o catálogo é preciso definir que ele será o **catálogo ativo** na sessão atual. Isso significa que, a partir deste ponto, todas as operações que referenciarem apenas o nome do schema (sem qualificar com o nome do catálogo) serão resolvidas automaticamente dentro de `mvpDataEngineer`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define 'mvpDataEngineer' como o catálogo ativo da sessão.
# MAGIC -- Comandos subsequentes não precisarão qualificar o nome do catálogo.
# MAGIC USE CATALOG mvpDataEngineer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 3 — Criação dos Schemas (Arquitetura Medalhão)
# MAGIC
# MAGIC A **arquitetura medalhão** organiza os dados em camadas:
# MAGIC
# MAGIC * **Staging**: Camada temporária de chegada, onde os arquivos copiados diretamente do Google Drive são armazenados sem nenhuma transformação. Funciona como uma área de transbordo antes do processamento.
# MAGIC * **Bronze**: Camada inicial onde os dados são inseridos em seu formato bruto, sem transformações significativas. Equivale à camada de ingestão.
# MAGIC * **Silver**: Camada intermediária onde os dados são limpos, normalizados e enriquecidos. Equivale à camada de processamento.
# MAGIC * **Gold**: Camada final onde os dados estão prontos para consumo analítico, já aggregados e modelados para relatórios e dashboards. Equivale à camada de apresentação.
# MAGIC
# MAGIC Cada schema será criado dentro do catálogo `mvpDataEngineer` já selecionado.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Camada staging: dados brutos copiados diretamente do Google Drive,
# MAGIC -- sem nenhuma transformação. Serve como área de chegada temporária
# MAGIC -- antes de serem movidos para a camada Bronze.
# MAGIC CREATE SCHEMA staging;
# MAGIC
# MAGIC -- Camada Bronze: dados brutos, conforme chegam da fonte.
# MAGIC CREATE SCHEMA bronze;
# MAGIC
# MAGIC -- Camada Silver: dados limpos e transformados.
# MAGIC CREATE SCHEMA silver;
# MAGIC
# MAGIC -- Camada Gold: dados prontos para análise e consumo.
# MAGIC CREATE SCHEMA gold;