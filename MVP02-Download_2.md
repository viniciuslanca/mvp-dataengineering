# MVP02 — Coleta e Ingestão na Camada Staging

Coleta os arquivos de origem no Google Drive via API e os materializa como tabelas Delta na camada `staging` do Data Lakehouse.

- **Posição no pipeline:** segundo notebook; requer `MVP01-Setup` executado.
- **Estratégia de carga:** *full refresh*.
- **Princípio da camada:** cópia fiel da origem; as colunas de negócio são gravadas como texto e tipadas na camada Silver.

## Etapa 1 — Preparação do ambiente

Instale as dependências no Databricks:

```python
%pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client openpyxl --quiet
dbutils.library.restartPython()
```

Após o reinício, importe as bibliotecas:

```python
import glob
import io
import json
import os

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructField, StructType, StringType
```

## Etapa 2 — Parâmetros de execução

```python
dbutils.widgets.text("catalogo", "mvpdataengineer", "Catálogo")
dbutils.widgets.text("schema", "staging", "Schema de destino")
dbutils.widgets.text("volume", "leituras", "Volume dos arquivos brutos")
dbutils.widgets.text("folder_id", "", "ID da pasta no Google Drive")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
FOLDER_ID = dbutils.widgets.get("folder_id")
BASE_PATH = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"

MAPA_ARQUIVO_TABELA = {
    "LEITURA_PLUVIOMETROS.xlsx": "leitura_pluviometros",
    "MEDICAO_BACIAS.xlsx": "medicao_bacias",
    "MEDICAO_LAJE.xlsx": "medicao_laje",
}
```

## Etapa 3 — Autenticação com o Google Drive

Não versione chaves privadas. Configure a Service Account em um secret scope do Databricks:

```bash
databricks secrets create-scope gdrive
databricks secrets put-secret gdrive service_account
```

O segredo `service_account` deve conter o JSON completo da Service Account. O escopo utilizado é somente leitura:

```python
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def carregar_credencial():
    try:
        bruto = dbutils.secrets.get(scope="gdrive", key="service_account")
        return json.loads(bruto)
    except Exception as erro:
        raise RuntimeError(
            "Configure o secret scope 'gdrive' com a chave 'service_account'."
        ) from erro


credentials = service_account.Credentials.from_service_account_info(
    carregar_credencial(), scopes=SCOPES
)
service = build("drive", "v3", credentials=credentials)
```

## Etapa 4 — Funções utilitárias

```python
def listar_arquivos_da_pasta(service, folder_id=None, tipo_mime=None):
    filtros = [
        "mimeType != 'application/vnd.google-apps.folder'",
        "trashed = false",
    ]
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
    caminho = os.path.join(destino, nome_arquivo)
    requisicao = service.files().get_media(fileId=file_id)

    with io.FileIO(caminho, "wb") as arquivo:
        downloader = MediaIoBaseDownload(arquivo, requisicao)
        concluido = False
        while not concluido:
            _, concluido = downloader.next_chunk()

    return caminho
```

## Etapa 5 — Preparação do Volume

```python
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}
    COMMENT 'Arquivos Excel brutos baixados do Google Drive'
""")

dbutils.fs.mkdirs(BASE_PATH)

for caminho in glob.glob(f"{BASE_PATH}/*.xlsx"):
    os.remove(caminho)
```

## Etapa 6 — Descoberta e download dos arquivos

```python
if not FOLDER_ID:
    pastas = service.files().list(
        q="mimeType = 'application/vnd.google-apps.folder' and trashed = false",
        pageSize=100,
        fields="files(id, name)",
    ).execute().get("files", [])

    if len(pastas) != 1:
        raise RuntimeError(
            "Informe o folder_id: não foi possível resolver uma única pasta."
        )
    FOLDER_ID = pastas[0]["id"]

arquivos = listar_arquivos_da_pasta(service, folder_id=FOLDER_ID)
esperados = set(MAPA_ARQUIVO_TABELA)
por_nome = {arquivo["name"]: arquivo for arquivo in arquivos}
faltando = esperados - por_nome.keys()

if faltando:
    raise RuntimeError(f"Arquivos esperados não encontrados: {sorted(faltando)}")

baixados = []
for nome_arquivo in esperados:
    arquivo = por_nome[nome_arquivo]
    caminho = baixar_arquivo(service, arquivo["id"], nome_arquivo, BASE_PATH)
    baixados.append({"arquivo": nome_arquivo, "caminho": caminho})

if len(baixados) != len(esperados):
    raise RuntimeError("Download incompleto; ingestão abortada.")
```

## Etapa 7 — Ingestão na camada Staging

Todos os campos de origem são carregados como `STRING`. Células vazias tornam-se `NULL`; o hífen literal (`-`) é preservado.

```python
for nome_arquivo, nome_tabela in MAPA_ARQUIVO_TABELA.items():
    caminho = os.path.join(BASE_PATH, nome_arquivo)
    pdf = pd.read_excel(caminho, sheet_name=0, dtype=str)
    pdf = pdf.astype(object).where(pd.notna(pdf), None)

    schema = StructType([
        StructField(coluna, StringType(), True)
        for coluna in pdf.columns
    ])
    sdf = spark.createDataFrame(pdf, schema=schema)
    sdf = (
        sdf.withColumn("data_ingestao", current_timestamp())
        .withColumn("origem", lit("google_drive"))
        .withColumn("arquivo_origem", lit(nome_arquivo))
    )

    tabela = f"{CATALOGO}.{SCHEMA}.{nome_tabela}"
    (
        sdf.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela)
    )
```

## Etapa 8 — Validação

```sql
SELECT table_name, column_name, data_type
FROM mvpdataengineer.information_schema.columns
WHERE table_schema = 'staging'
ORDER BY table_name, ordinal_position;
```

Valide também a quantidade de registros e os metadados `data_ingestao`, `origem` e `arquivo_origem` em cada tabela.

## Tabelas geradas

| Arquivo de origem | Tabela Delta |
|---|---|
| `LEITURA_PLUVIOMETROS.xlsx` | `mvpdataengineer.staging.leitura_pluviometros` |
| `MEDICAO_BACIAS.xlsx` | `mvpdataengineer.staging.medicao_bacias` |
| `MEDICAO_LAJE.xlsx` | `mvpdataengineer.staging.medicao_laje` |

> **Segurança:** nunca preencha ou versione `private_key` no repositório. Use o secret scope `gdrive` e revogue qualquer chave que tenha sido exposta.

Carga concluída. Prossiga para o notebook **MVP03-Bronze**.
