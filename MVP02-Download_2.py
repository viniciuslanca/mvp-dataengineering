# Databricks notebook source
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
# MAGIC ## Etapa 1 — Preparação do ambiente
# MAGIC
# MAGIC Bibliotecas necessárias para autenticação OAuth 2.0 com o Google, acesso à Drive API e
# MAGIC leitura de arquivos Excel:
# MAGIC
# MAGIC | Biblioteca | Função |
# MAGIC |---|---|
# MAGIC | `google-auth` | Autenticação com Google Cloud |
# MAGIC | `google-auth-oauthlib` | Fluxo OAuth 2.0 |
# MAGIC | `google-auth-httplib2` | Transporte HTTP |
# MAGIC | `google-api-python-client` | Cliente da Google Drive API |
# MAGIC | `openpyxl` | Leitura de arquivos `.xlsx` |
# MAGIC
# MAGIC `dbutils.library.restartPython()` reinicia o interpretador para que os pacotes recém-instalados
# MAGIC fiquem disponíveis aos imports. Por isso os imports ficam na célula seguinte, e não nesta.

# COMMAND ----------

# MAGIC %pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client openpyxl --quiet

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
# MAGIC ## Etapa 2 — Parâmetros de execução
# MAGIC
# MAGIC Os parâmetros ficam em widgets no topo do notebook, de modo que o código não carrega
# MAGIC identificadores fixos e pode ser reaproveitado por outra pessoa com o Drive dela.

# COMMAND ----------

dbutils.widgets.text("catalogo", "mvpdataengineer", "Catálogo")
dbutils.widgets.text("schema",   "staging",         "Schema de destino")
dbutils.widgets.text("volume",   "leituras",        "Volume dos arquivos brutos")
dbutils.widgets.text("folder_id", "",               "ID da pasta no Google Drive")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA   = dbutils.widgets.get("schema")
VOLUME   = dbutils.widgets.get("volume")
FOLDER_ID = dbutils.widgets.get("folder_id")

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
# MAGIC ## Etapa 3 — Autenticação com o Google Drive
# MAGIC
# MAGIC A autenticação usa uma **Service Account** do Google Cloud: uma identidade de aplicação
# MAGIC que permite acesso automatizado sem intervenção interativa do usuário.
# MAGIC
# MAGIC Como foi configurada: criação de um projeto no Google Cloud, habilitação da Google Drive
# MAGIC API, geração de uma Service Account com chave JSON e compartilhamento da pasta do Drive
# MAGIC com o e-mail dessa conta. O escopo concedido é `drive.readonly` — somente leitura.
# MAGIC
# MAGIC ### ⚠️ Credenciais
# MAGIC
# MAGIC **A chave privada da Service Account não deve ser versionada.** Antes de publicar este
# MAGIC notebook, apague o conteúdo do bloco `CREDENCIAL_JSON` abaixo e revogue a chave no
# MAGIC Google Cloud Console (IAM & Admin → Service Accounts → Keys).
# MAGIC
# MAGIC O código tenta primeiro ler a credencial de um **secret scope** do Databricks, que é a
# MAGIC forma correta. O bloco embutido existe apenas como alternativa de execução direta.

# COMMAND ----------

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# ---------------------------------------------------------------------------
# OPÇÃO A — Databricks Secrets (recomendada; usada automaticamente se existir)
#
#   databricks secrets create-scope gdrive
#   databricks secrets put-secret  gdrive service_account
#   (cole o conteúdo do arquivo JSON da Service Account)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# OPÇÃO B — Credencial embutida, apenas para execução local.
#
#   >>> COLE AQUI o conteúdo do arquivo JSON da Service Account <<<
#   >>> APAGUE antes de commitar e revogue a chave no Google Cloud <<<
# ---------------------------------------------------------------------------
CREDENCIAL_JSON = {
    "type": "service_account",
    "project_id": "",
    "private_key_id": "",
    "private_key": "",
    "client_email": "",
    "client_id": "",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "",
    "universe_domain": "googleapis.com",
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
        print("⚠️  Credencial obtida do bloco embutido no notebook.")
        print("    Apague o bloco e revogue a chave antes de publicar o repositório.")
        return CREDENCIAL_JSON

    raise RuntimeError(
        "Credencial não encontrada.\n"
        "Configure o secret scope 'gdrive' com a chave 'service_account' contendo o JSON "
        "da Service Account do Google Cloud, ou preencha o bloco CREDENCIAL_JSON desta célula.\n"
        "Instruções detalhadas no README do repositório."
    )


credentials = service_account.Credentials.from_service_account_info(
    carregar_credencial(), scopes=SCOPES
)

print("Credenciais configuradas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 4 — Funções utilitárias

# COMMAND ----------

def listar_arquivos_da_pasta(service, folder_id=None, tipo_mime=None):
    """Lista os arquivos de uma pasta do Google Drive.

    Args:
        service:   objeto de serviço da Google Drive API.
        folder_id: ID da pasta. None consulta a raiz acessível à Service Account.
        tipo_mime: filtro opcional por tipo MIME.

    Returns:
        list[dict]: arquivos com id, name, mimeType, size e datas.
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
# MAGIC ## Etapa 6 — Descoberta da pasta de origem
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
# MAGIC ## Etapa 7 — Download dos arquivos
# MAGIC
# MAGIC Baixa os três arquivos esperados. Se algum não for encontrado ou o download falhar, a
# MAGIC execução é interrompida — não faz sentido seguir para a ingestão com carga parcial.

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
# MAGIC ## Etapa 8 — Ingestão na camada Staging
# MAGIC
# MAGIC Cada arquivo Excel é lido e gravado como tabela Delta.
# MAGIC
# MAGIC ### Decisões de implementação
# MAGIC
# MAGIC **Leitura com `dtype=str`.** O pandas infere o tipo de cada coluna a partir dos valores
# MAGIC presentes, o que torna o schema dependente do conteúdo: uma coluna de texto inteiramente
# MAGIC vazia é inferida como numérica, e passa a ser texto assim que alguém a preencher. Forçar
# MAGIC tudo como texto garante que o schema da staging seja **estável entre execuções**.
# MAGIC
# MAGIC **Schema Spark explícito.** Em vez de deixar o Spark inferir a partir do pandas, o schema
# MAGIC é construído com todas as colunas como `StringType`. Nenhuma inferência participa da carga.
# MAGIC
# MAGIC **Preservação de vazio versus hífen.** Nos arquivos de origem, a ausência de valor aparece
# MAGIC ora como célula vazia, ora como o caractere `-`. São representações diferentes da mesma
# MAGIC ausência e o tratamento delas é da camada Silver; aqui as duas são preservadas como
# MAGIC vieram — célula vazia vira `NULL`, hífen permanece a string `-`.
# MAGIC
# MAGIC **`overwriteSchema` em vez de `mergeSchema`.** Em carga full refresh o schema deve ser
# MAGIC substituído. Com `mergeSchema`, colunas de versões anteriores do notebook sobrevivem
# MAGIC preenchidas com NULL, poluindo a tabela com campos que o código nunca grava.
# MAGIC
# MAGIC ### Metadados de rastreabilidade acrescentados
# MAGIC
# MAGIC | Coluna | Conteúdo |
# MAGIC |---|---|
# MAGIC | `data_ingestao` | Timestamp da carga na staging |
# MAGIC | `origem` | Sistema de origem (`google_drive`) |
# MAGIC | `arquivo_origem` | Nome do arquivo Excel que deu origem à linha |

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
