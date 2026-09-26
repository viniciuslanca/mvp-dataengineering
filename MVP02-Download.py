# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MVP02 — Coleta e Ingestão na Camada Staging
# MAGIC
# MAGIC Coleta os arquivos de origem no Google Drive via API e os materializa como tabelas Delta
# MAGIC na camada `staging` do Data Lakehouse.
# MAGIC
# MAGIC **Posição no pipeline:** segundo notebook. Requer `MVP01-Setup` executado.
# MAGIC
# MAGIC **Estratégia de carga:** *full refresh*. Cada execução limpa o Volume, rebaixa os três
# MAGIC arquivos e substitui integralmente as tabelas de staging. O volume de dados (pouco mais
# MAGIC de 4 mil registros somados) não justifica carga incremental, e o refresh completo
# MAGIC simplifica a reprodutibilidade.
# MAGIC
# MAGIC **Princípio da camada:** a staging é uma cópia fiel da origem. Todas as colunas são
# MAGIC gravadas como texto, sem inferência de tipo. A tipagem é responsabilidade da camada
# MAGIC Silver, onde está documentada e versionada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Sobre este Notebook
# MAGIC
# MAGIC Este notebook implementa o processo completo de **ingestão de dados** seguindo a **Arquitetura Medalhão (Medallion Architecture)**.
# MAGIC
# MAGIC ### 🎯 Objetivo
# MAGIC Baixar dados de planilhas Excel do Google Drive e carregá-los na **camada Staging** do Data Lakehouse.
# MAGIC As demais camadas (Bronze, Silver e Gold) serão implementadas em notebooks subsequentes.
# MAGIC
# MAGIC ### Arquitetura Medalhão
# MAGIC
# MAGIC A arquitetura medalhão organiza os dados em camadas:
# MAGIC
# MAGIC * **📂 Staging (Chegada)**: Dados brutos copiados diretamente do Google Drive (Este notebook)
# MAGIC   - Cópia fiel da origem, sem nenhuma transformação
# MAGIC   - Área temporária de transbordo
# MAGIC   - Inclui metadados de ingestão
# MAGIC
# MAGIC * **🥉 Bronze (Raw)**: Dados brutos com metadados de rastreabilidade (Notebook MVP03-Bronze)
# MAGIC   - Cópia fiel da origem com controle de versão
# MAGIC   - Mantém histórico completo
# MAGIC   - Implementada em notebook próprio
# MAGIC   
# MAGIC * **🥈 Silver (Cleaned)**: Dados limpos e validados (Notebook MVP04-Silver)
# MAGIC   - Dados padronizados
# MAGIC   - Qualidade garantida
# MAGIC   - Implementada em notebook próprio
# MAGIC   
# MAGIC * **🥇 Gold (Curated)**: Dados agregados e otimizados (Notebook MVP05-Gold)
# MAGIC   - Métricas de negócio
# MAGIC   - Dados prontos para consumo
# MAGIC   - Implementada em notebook próprio

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Etapa 1: Preparação do Ambiente
# MAGIC
# MAGIC Instalar as bibliotecas Python necessárias para autenticação OAuth 2.0 com o Google, acesso à API do Google Drive e Leitura de arquivos Excel(.xlsx)
# MAGIC
# MAGIC ## Bibliotecas instaladas:
# MAGIC * `google-auth` - Autenticação com Google Cloud
# MAGIC * `google-auth-oauthlib` - Fluxo OAuth 2.0
# MAGIC * `google-auth-httplib2` - Transporte HTTP
# MAGIC * `google-api-python-client` - Cliente da API do Google Drive
# MAGIC * `openpyxl` - Leitura de arquivos Excel
# MAGIC
# MAGIC `dbutils.library.restartPython()` reinicia o interpretador para que os pacotes recém-instalados
# MAGIC fiquem disponíveis aos imports. Por isso os imports ficam na célula seguinte, e não nesta.
# MAGIC

# COMMAND ----------

# Instalar bibliotecas para Google Drive e Excel
%pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client openpyxl --quiet

# COMMAND ----------

# Reinicia o interpretador para que os pacotes instalados acima fiquem disponíveis.
# Atenção: isso limpa todas as variáveis da sessão, por isso vem antes de qualquer definição.
dbutils.library.restartPython()

# COMMAND ----------

# Imports concentrados em uma única célula, após o restartPython.

# Google Cloud / Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Biblioteca padrão
import io
import os
import json
import glob

# Processamento
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType

print("Bibliotecas importadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Etapa 2: Preparação do Armazenamento
# MAGIC
# MAGIC * Definir o Catalogo e schema que será usado. 
# MAGIC * Criar o volume dentro do schema selecionado. 
# MAGIC
# MAGIC Os arquivos Excel baixados do Google Drive são armazenados em um **Volume do Unity Catalog** (`mvpDataEngineer.staging.leituras`), garantindo persistência, governança e disponibilidade entre sessões.

# COMMAND ----------


CATALOGO = "mvpdataengineer"
SCHEMA   = "staging"
VOLUME   = "leituras"
FOLDER_ID = ""

BASE_PATH = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"

# Mapeamento explícito arquivo de origem -> tabela de destino.
# Evita que uma planilha renomeada no Drive crie silenciosamente uma tabela nova.
MAPA_ARQUIVO_TABELA = {
    "LEITURA_PLUVIOMETROS.xlsx": "leitura_pluviometros",
    "MEDICAO_BACIAS.xlsx":       "medicao_bacias",
    "MEDICAO_LAJE.xlsx":         "medicao_laje",
}

print(f"Catálogo ......: {CATALOGO}")
print(f"Schema ........: {SCHEMA}")
print(f"Volume ........: {BASE_PATH}")
print(f"Arquivos ......: {list(MAPA_ARQUIVO_TABELA)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Etapa 3: Autenticação com Google Drive
# MAGIC
# MAGIC Para essa etapa foi utilizado o **Service Account** que é uma conta de serviço do Google Cloud que permite autenticação automática sem intervenção do usuário.
# MAGIC
# MAGIC ##### Como funciona?
# MAGIC Cria-se um projeto no Google Cloud, habilita-se a Google Drive API, gera-se uma Service Account com chave JSON e compartilha-se a pasta do Drive com essa conta de serviço.
# MAGIC
# MAGIC
# MAGIC ***⚠️ Como não é um ambiente produtivo, as credenciais foram utilizadas diretamente no código, mas por segurança poreria ser utilizado o Databricks Secrets.***

# COMMAND ----------

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


CREDENCIAL_JSON = {
#--------------------------------------------------
# 🔐 Por segurança as credenciais foram retiradas.
#--------------------------------------------------
}

# ---------------------------------------------------------------------------

def carregar_credencial():
    """Retorna o dicionário de credenciais, priorizando o secret scope."""
    try:
        bruto = dbutils.secrets.get(scope="gdrive", key="service_account")
        print("Credencial obtida do secret scope 'gdrive'.")
        return json.loads(bruto)
    except Exception:
        pass

    if CREDENCIAL_JSON.get("private_key"):
        return CREDENCIAL_JSON

    raise RuntimeError(
        "Credencial não encontrada."
    )


credentials = service_account.Credentials.from_service_account_info(
    carregar_credencial(), scopes=SCOPES
)

print("Credenciais configuradas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Etapa 3: Funções de Utilitário
# MAGIC
# MAGIC ###### 1️⃣ `list_files_in_folder()`
# MAGIC **Propósito:** Listar arquivos de uma pasta
# MAGIC
# MAGIC **Parâmetros:**
# MAGIC * `service` - Conexão com Google Drive API
# MAGIC * `folder_id` - ID da pasta (opcional)
# MAGIC * `file_type` - Filtro por tipo MIME (opcional)
# MAGIC
# MAGIC **Retorna:** Lista de arquivos com metadados (id, nome, tamanho, data)
# MAGIC
# MAGIC ###### 2️⃣ `download_file()`
# MAGIC **Propósito:** Baixar um arquivo específico
# MAGIC
# MAGIC **Parâmetros:**
# MAGIC * `service` - Conexão com Google Drive API
# MAGIC * `file_id` - ID do arquivo
# MAGIC * `file_name` - Nome do arquivo
# MAGIC * `destination_path` - Caminho local de destino
# MAGIC
# MAGIC **Retorna:** Caminho completo do arquivo baixado

# COMMAND ----------

def listar_arquivos_da_pasta(service, folder_id=None, tipo_mime=None):
    """
        Baixa um arquivo do Google Drive
    
    Args:
        service: Serviço do Google Drive API
        file_id: ID do arquivo no Google Drive
        file_name: Nome do arquivo
        destination_path: Caminho de destino local
    
    Returns:
        Caminho completo do arquivo baixado
    """
    filtros = ["mimeType != 'application/vnd.google-apps.folder'", "trashed = false"]
    if folder_id:
        filtros.append(f"'{folder_id}' in parents")
    if tipo_mime:
        filtros.append(f"mimeType = '{tipo_mime}'")

    resultado = service.files().list(
        q=" and ".join(filtros),
        pageSize=100,
        fields="files(id, name, mimeType, size, createdTime, modifiedTime)",
    ).execute()

    return resultado.get("files", [])


def baixar_arquivo(service, file_id, nome_arquivo, destino):
    """Baixa um arquivo do Google Drive para o caminho de destino.

    Returns:
        str | None: caminho completo do arquivo baixado, ou None em caso de falha.
    """
    try:
        requisicao = service.files().get_media(fileId=file_id)
        caminho = os.path.join(destino, nome_arquivo)

        with io.FileIO(caminho, "wb") as arquivo:
            downloader = MediaIoBaseDownload(arquivo, requisicao)
            concluido = False
            while not concluido:
                status, concluido = downloader.next_chunk()
                if status:
                    print(f"    {int(status.progress() * 100):3d}%  {nome_arquivo}")

        print(f"  ✓ {nome_arquivo}")
        return caminho

    except Exception as erro:
        print(f"  ✗ Falha ao baixar {nome_arquivo}: {erro}")
        return None


print("Funções utilitárias definidas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 5 — Preparação do Volume
# MAGIC
# MAGIC Os arquivos são armazenados em um **Volume do Unity Catalog**, e não no sistema de
# MAGIC arquivos efêmero do cluster. O Volume garante persistência entre sessões, governança de
# MAGIC acesso e rastreabilidade — e é a área de armazenamento disponível no Databricks Free
# MAGIC Edition, onde o DBFS não está acessível.
# MAGIC
# MAGIC O Volume é limpo antes de cada carga. Sem isso, uma falha no download deixaria os
# MAGIC arquivos da execução anterior no diretório, e eles seriam reingeridos como se fossem novos.

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}
    COMMENT 'Arquivos Excel brutos baixados do Google Drive, antes da ingestão nas tabelas Delta da camada Staging'
""")

dbutils.fs.mkdirs(BASE_PATH)

# Limpeza dos arquivos de execuções anteriores
residuais = glob.glob(f"{BASE_PATH}/*.xlsx")
for caminho in residuais:
    os.remove(caminho)

print(f"Volume pronto: {BASE_PATH}")
print(f"Arquivos residuais removidos: {len(residuais)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍  Etapa 6 — Descoberta da pasta de origem
# MAGIC
# MAGIC Cada pasta do Google Drive tem um identificador único (*folder ID*) usado pela API.
# MAGIC A célula abaixo lista as pastas visíveis à Service Account — que são apenas aquelas
# MAGIC explicitamente compartilhadas com ela.
# MAGIC
# MAGIC Se o widget `folder_id` estiver vazio, a pasta é localizada automaticamente pelo nome.

# COMMAND ----------

service = build("drive", "v3", credentials=credentials)

resultado = service.files().list(
    q="mimeType = 'application/vnd.google-apps.folder' and trashed = false",
    pageSize=100,
    fields="files(id, name)",
).execute()

pastas = resultado.get("files", [])

print(f"Pastas acessíveis à Service Account: {len(pastas)}\n")
print(f"{'Nome':<40} | Folder ID")
print("-" * 80)
for pasta in pastas:
    print(f"{pasta['name']:<40} | {pasta['id']}")

# Resolve o folder_id quando o widget não foi preenchido
if not FOLDER_ID:
    if len(pastas) == 1:
        FOLDER_ID = pastas[0]["id"]
        print(f"\nfolder_id resolvido automaticamente: {FOLDER_ID} ({pastas[0]['name']})")
    else:
        raise RuntimeError(
            "Informe o folder_id no widget: mais de uma pasta acessível à Service Account."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Etapa 7 — Download dos arquivos
# MAGIC
# MAGIC A célula abaixo executa o **download automatizado** dos arquivos Excel da pasta `DataEngineer` no Google Drive. As principais ações são:
# MAGIC
# MAGIC 1. **Configura o serviço do Google Drive** usando as credenciais já autenticadas.
# MAGIC 2. **Define a pasta de origem** pelo `folder_id` identificado na etapa anterior.
# MAGIC 3. **Especifica os 3 arquivos-alvo**:
# MAGIC    - `LEITURA_PLUVIOMETROS.xlsx` — dados de pluviometria
# MAGIC    - `MEDICAO_BACIAS.xlsx` — dados das bacias de evaporação
# MAGIC    - `MEDICAO_LAJE.xlsx` — medições de cristalizadores
# MAGIC 4. **Lista todos os arquivos** da pasta usando a função `list_files_in_folder()`.
# MAGIC 5. **Filtra** apenas os arquivos desejados e avisa caso algum não seja encontrado.
# MAGIC 6. **Baixa cada arquivo** com a função `download_file()`, salvando no diretório temporário (`base_path`).
# MAGIC 7. **Registra metadados** de cada download (nome, caminho, tamanho em KB e data de modificação).
# MAGIC 8. **Exibe um resumo final** em formato de Spark DataFrame com os arquivos baixados.
# MAGIC
# MAGIC #### ✅ Validação:
# MAGIC Se algum arquivo não for encontrado, o script avisará quais estão faltando.

# COMMAND ----------

print("Buscando arquivos na pasta de origem...\n")

arquivos_na_pasta = listar_arquivos_da_pasta(service, folder_id=FOLDER_ID)
print(f"Arquivos na pasta: {len(arquivos_na_pasta)}")
for a in arquivos_na_pasta:
    print(f"  - {a['name']}")

esperados = set(MAPA_ARQUIVO_TABELA)
a_baixar = [a for a in arquivos_na_pasta if a["name"] in esperados]

faltando = esperados - {a["name"] for a in a_baixar}
if faltando:
    raise RuntimeError(f"Arquivos esperados não encontrados na pasta de origem: {sorted(faltando)}")

print(f"\nBaixando {len(a_baixar)} arquivo(s)...\n")

baixados = []
for arquivo in a_baixar:
    caminho = baixar_arquivo(service, arquivo["id"], arquivo["name"], BASE_PATH)
    if caminho:
        baixados.append({
            "arquivo":      arquivo["name"],
            "caminho":      caminho,
            "tamanho_kb":   round(int(arquivo.get("size", 0)) / 1024, 2),
            "modificado_em": arquivo.get("modifiedTime", "N/A"),
        })

# Guarda contra carga parcial
if len(baixados) != len(esperados):
    raise RuntimeError(
        f"Download incompleto: {len(baixados)} de {len(esperados)} arquivos. "
        "Ingestão abortada para não gerar carga parcial."
    )

print(f"\nDownload concluído: {len(baixados)} arquivo(s).")
display(spark.createDataFrame(baixados))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Etapa 8: Ingestão na Camada Staging
# MAGIC
# MAGIC Objetivo: Carregar arquivos Excel do Google Drive para tabelas Delta na camada Staging, mantendo os dados brutos + metadados de rastreabilidade.
# MAGIC
# MAGIC As transformações para Bronze, Silver e Gold serão feitas em notebooks próprios.
# MAGIC
# MAGIC ### 🎯 Tabelas criadas:
# MAGIC 1. `mvpDataEngineer.staging.leitura_pluviometros`
# MAGIC 2. `mvpDataEngineer.staging.medicao_bacias`
# MAGIC 3. `mvpDataEngineer.staging.medicao_laje`
# MAGIC
# MAGIC ### 📊 Metadados adicionados:
# MAGIC * **data_ingestao** - Timestamp da carga
# MAGIC * **origem** - Fonte dos dados (google_drive)
# MAGIC * **arquivo_origem** - Nome do arquivo original

# COMMAND ----------

print("Iniciando ingestão na camada Staging...\n")
print("=" * 80)

resumo_ingestao = []

for nome_arquivo, nome_tabela in MAPA_ARQUIVO_TABELA.items():
    caminho = os.path.join(BASE_PATH, nome_arquivo)
    print(f"\n{nome_arquivo}")

    # 1. Leitura sem inferência de tipo
    pdf = pd.read_excel(caminho, sheet_name=0, dtype=str)

    # 2. NaN do pandas -> None, para virar NULL no Spark.
    #    O hífen literal '-' é preservado como texto.
    pdf = pdf.astype(object).where(pd.notna(pdf), None)

    print(f"  Linhas lidas .....: {len(pdf)}")
    print(f"  Colunas de origem : {len(pdf.columns)}")

    # 3. Schema Spark explícito: tudo texto, nada inferido
    schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    sdf = spark.createDataFrame(pdf, schema=schema)

    # 4. Metadados de rastreabilidade
    sdf = (sdf
           .withColumn("data_ingestao", current_timestamp())
           .withColumn("origem", lit("google_drive"))
           .withColumn("arquivo_origem", lit(nome_arquivo)))

    # 5. Gravação full refresh
    tabela = f"{CATALOGO}.{SCHEMA}.{nome_tabela}"
    (sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela))

    total = sdf.count()
    print(f"  Tabela ...........: {tabela}")
    print(f"  Registros gravados: {total}")

    resumo_ingestao.append({
        "arquivo": nome_arquivo,
        "tabela": nome_tabela,
        "registros": total,
        "colunas": len(sdf.columns),
    })

print("\n" + "=" * 80)
print("Ingestão concluída.")
display(spark.createDataFrame(resumo_ingestao))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 9 — Documentação das tabelas no Unity Catalog
# MAGIC
# MAGIC Comentários registrados no catálogo tornam as tabelas autodescritivas: eles aparecem no
# MAGIC Catalog Explorer, no autocomplete do editor SQL e nas APIs de metadados.
# MAGIC
# MAGIC As descrições nesta camada registram o significado do campo **na origem**, sem antecipar
# MAGIC o tratamento que a Silver aplicará.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mvpdataengineer.staging.leitura_pluviometros IS
# MAGIC   'Camada de chegada. Leituras pluviométricas das estações da salina, exportadas do formulário eletrônico de controle produtivo. Todas as colunas como texto, sem transformação.';
# MAGIC
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.PROTOCOLO IS 'Identificador da solicitação no formulário de origem. Uma solicitação agrupa várias leituras.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.SOLICDATA IS 'Data e hora do lançamento no sistema, no formato dd/MM/yyyy HH:mm:ss. Distinta da data da leitura.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.STATUS IS 'Situação no fluxo de aprovação do formulário.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.UNIDADE IS 'Unidade da empresa à qual a estação pertence.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.ESTACAO IS 'Rótulo da estação pluviométrica conforme registrado no formulário.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.DATAREGISTRO IS 'Data da leitura em campo, no formato dd/MM/yyyy.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.MILIMETROS IS 'Precipitação medida, em milímetros. Decimal com vírgula. Ausência representada por hífen.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.OBSERVACOES IS 'Campo livre do formulário.';
# MAGIC
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.data_ingestao IS 'Timestamp da carga na camada Staging.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.origem IS 'Sistema de origem dos dados.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.leitura_pluviometros.arquivo_origem IS 'Nome do arquivo Excel que deu origem à linha.';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mvpdataengineer.staging.medicao_bacias IS
# MAGIC   'Camada de chegada. Medição diária das bacias de evaporação pelo método de reposição até marca de referência, exportada do formulário eletrônico de controle produtivo. Todas as colunas como texto, sem transformação.';
# MAGIC
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.PROTOCOLO IS 'Identificador da solicitação no formulário de origem. Uma solicitação agrupa vários dias e os dois pontos de medição.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.SOLICDATA IS 'Data e hora do lançamento no sistema, no formato dd/MM/yyyy HH:mm:ss. Distinta da data da medição.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.STATUS IS 'Situação no fluxo de aprovação do formulário.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.UNIDADE IS 'Unidade da empresa à qual a bacia pertence.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.BACIA IS 'Ponto de medição de evaporação.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.DATAREGISTRO IS 'Data da medição em campo, no formato dd/MM/yyyy.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.AGUADOCE_COLOCADO IS 'Volume de água doce reposto até a marca de referência, em litros. Corresponde à evaporação do dia. Ausência representada por hífen.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.AGUADOCE_RETIRADO IS 'Volume de água doce retirado por excedente de chuva, em litros. Mutuamente exclusivo com AGUADOCE_COLOCADO. Ausência representada por hífen.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.SALMOURA_COLOCADO IS 'Volume de salmoura reposto até a marca de referência, em litros. Corresponde à evaporação do dia. Ausência representada por hífen.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.SALMOURA_RETIRADO IS 'Volume de salmoura retirado por excedente de chuva, em litros. Mutuamente exclusivo com SALMOURA_COLOCADO. Ausência representada por hífen.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.SALMOURA_DENSIDADE IS 'Concentração da salmoura em graus Baumé (°Bé), medida com densímetro. Decimal com vírgula. Ausência representada por hífen.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.OBSERVACOES IS 'Campo livre do formulário.';
# MAGIC
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.data_ingestao IS 'Timestamp da carga na camada Staging.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.origem IS 'Sistema de origem dos dados.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_bacias.arquivo_origem IS 'Nome do arquivo Excel que deu origem à linha.';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mvpdataengineer.staging.medicao_laje IS
# MAGIC   'Camada de chegada. Medição mensal da laje de sal dos cristalizadores por meio de 8 réguas, exportada do formulário eletrônico de controle produtivo. Todas as colunas como texto, sem transformação.';
# MAGIC
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.PROTOCOLO IS 'Identificador da solicitação no formulário de origem. Uma solicitação agrupa os 25 cristalizadores de uma campanha de medição.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.SOLICDATA IS 'Data e hora do lançamento no sistema, no formato dd/MM/yyyy HH:mm:ss. Distinta da data da medição.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.STATUS IS 'Situação no fluxo de aprovação do formulário.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.UNIDADE IS 'Unidade da empresa à qual o cristalizador pertence.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.DATAMEDICAO IS 'Data da medição em campo, no formato dd/MM/yyyy.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.CRISTALIZADOR IS 'Identificador do cristalizador medido.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.AREAHECTARES IS 'Área do cristalizador em hectares. Constante por cristalizador.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.REGUA_1 IS 'Altura livre da régua 1 acima da laje, em centímetros. Régua de 1 metro, portanto 100 é o limite superior da leitura.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.REGUA_2 IS 'Altura livre da régua 2 acima da laje, em centímetros. Régua de 1 metro, portanto 100 é o limite superior da leitura.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.REGUA_3 IS 'Altura livre da régua 3 acima da laje, em centímetros. Régua de 1 metro, portanto 100 é o limite superior da leitura.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.REGUA_4 IS 'Altura livre da régua 4 acima da laje, em centímetros. Régua de 1 metro, portanto 100 é o limite superior da leitura.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.REGUA_5 IS 'Altura livre da régua 5 acima da laje, em centímetros. Régua de 1 metro, portanto 100 é o limite superior da leitura.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.REGUA_6 IS 'Altura livre da régua 6 acima da laje, em centímetros. Régua de 1 metro, portanto 100 é o limite superior da leitura.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.REGUA_7 IS 'Altura livre da régua 7 acima da laje, em centímetros. Régua de 1 metro, portanto 100 é o limite superior da leitura.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.REGUA_8 IS 'Altura livre da régua 8 acima da laje, em centímetros. Régua de 1 metro, portanto 100 é o limite superior da leitura.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.OBSERVACOES IS 'Campo livre do formulário. Registra eventualmente os dias de fabricação e a ocorrência de colheita.';
# MAGIC
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.data_ingestao IS 'Timestamp da carga na camada Staging.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.origem IS 'Sistema de origem dos dados.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.staging.medicao_laje.arquivo_origem IS 'Nome do arquivo Excel que deu origem à linha.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 10 — Validação da carga
# MAGIC
# MAGIC Confere o volume ingerido, a origem e o momento da carga de cada tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'leitura_pluviometros' AS tabela, COUNT(*) AS registros,
# MAGIC        MIN(arquivo_origem) AS arquivo, MAX(data_ingestao) AS ingerido_em
# MAGIC FROM mvpdataengineer.staging.leitura_pluviometros
# MAGIC UNION ALL
# MAGIC SELECT 'medicao_bacias', COUNT(*), MIN(arquivo_origem), MAX(data_ingestao)
# MAGIC FROM mvpdataengineer.staging.medicao_bacias
# MAGIC UNION ALL
# MAGIC SELECT 'medicao_laje', COUNT(*), MIN(arquivo_origem), MAX(data_ingestao)
# MAGIC FROM mvpdataengineer.staging.medicao_laje
# MAGIC ORDER BY tabela;

# COMMAND ----------

# MAGIC %md
# MAGIC Confirmação de que nenhuma coluna foi tipada na chegada: todas devem aparecer como `STRING`,
# MAGIC exceto os três metadados acrescentados pelo pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, column_name, data_type
# MAGIC FROM mvpdataengineer.information_schema.columns
# MAGIC WHERE table_schema = 'staging'
# MAGIC ORDER BY table_name, ordinal_position;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mvpdataengineer.staging.medicao_bacias LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mvpdataengineer.staging.medicao_laje LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mvpdataengineer.staging.leitura_pluviometros LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Carga concluída. Prossiga para o notebook **MVP03-Bronze**.