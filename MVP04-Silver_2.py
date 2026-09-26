# Databricks notebook source
# MAGIC %md
# MAGIC # MVP04 — Camada Silver
# MAGIC
# MAGIC Lê a camada `bronze`, diagnostica a qualidade dos dados, aplica os tratamentos
# MAGIC necessários e persiste o resultado na camada `silver`.
# MAGIC
# MAGIC **Posição no pipeline:** quarto notebook. Requer `MVP03-Bronze` executado.
# MAGIC
# MAGIC **Princípio da camada:** a Silver mantém a granularidade da origem, mas entrega o dado
# MAGIC limpo, tipado e confiável. É aqui que os problemas de qualidade são identificados e
# MAGIC resolvidos — nenhuma camada posterior precisa tratá-los de novo.
# MAGIC
# MAGIC ## Estrutura do notebook
# MAGIC
# MAGIC | Etapa | Conteúdo |
# MAGIC |---|---|
# MAGIC | 1 | Parâmetros e funções auxiliares |
# MAGIC | 2 | **Diagnóstico de qualidade sobre a Bronze** — antes de qualquer tratamento |
# MAGIC | 3 | Transformações e correções, tabela por tabela |
# MAGIC | 4 | **Validação pós-tratamento** — os mesmos testes, agora sobre a Silver |
# MAGIC | 5 | Documentação no Unity Catalog |
# MAGIC | 6 | Resumo do tratamento aplicado |
# MAGIC
# MAGIC A separação entre as etapas 2 e 4 é deliberada: rodar os mesmos testes antes e depois
# MAGIC evidencia tanto o problema encontrado quanto a sua resolução.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 1 — Parâmetros e funções auxiliares
# MAGIC
# MAGIC ### Colunas descartadas nesta camada
# MAGIC
# MAGIC Cinco colunas presentes nas três tabelas de origem **não seguem para a Silver**, por não
# MAGIC terem valor analítico:
# MAGIC
# MAGIC | Coluna | Motivo do descarte |
# MAGIC |---|---|
# MAGIC | `PROTOCOLO` | Identificador do formulário de origem. É metadado do sistema de lançamento, não do fenômeno medido. Nenhuma pergunta de negócio depende dele. |
# MAGIC | `SOLICDATA` | Data do lançamento no sistema, não da medição. Irrelevante para a análise. |
# MAGIC | `STATUS` | Situação no fluxo de aprovação. **Usado como filtro antes de ser descartado** — ver abaixo. |
# MAGIC | `UNIDADE` | Constante em todas as linhas das três tabelas. Coluna sem variação não carrega informação. |
# MAGIC | `OBSERVACOES` | Texto livre, vazio em duas das três fontes e preenchido em 4 de 226 linhas na terceira. Volume insuficiente para tratamento estruturado. |
# MAGIC
# MAGIC **`PROTOCOLO` e `SOLICDATA` são usados durante o processamento e descartados ao final.**
# MAGIC Eles são necessários para dois tratamentos: `SOLICDATA` serve de critério de desempate na
# MAGIC deduplicação (o lançamento mais recente corrige o anterior), e `PROTOCOLO` permite
# MAGIC identificar registros atribuídos à entidade ou à data errada. Cumprida essa função, saem.
# MAGIC
# MAGIC **`STATUS` é aplicado como filtro antes do descarte.** Apenas registros com
# MAGIC `Solicitação Finalizada` — homologados pela operação — seguem adiante. Depois do filtro,
# MAGIC a coluna passa a ter valor único e é removida pelo mesmo motivo de `UNIDADE`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "mvpdataengineer", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")

STATUS_HOMOLOGADO = "Solicitação Finalizada"

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql("USE SCHEMA silver")

print(f"Catálogo: {CATALOGO}")
print(f"Status homologado: {STATUS_HOMOLOGADO}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def texto_para_numero(coluna, tipo="double"):
    """Converte texto da origem em número.

    Trata as duas formas de ausência presentes nos arquivos (hífen e célula vazia)
    e a vírgula como separador decimal.
    """
    limpo = F.trim(F.col(coluna))
    return (F.when(limpo.isin("-", ""), None)
             .otherwise(F.regexp_replace(limpo, ",", ".").cast(tipo)))


def chave_duplicada(df, chaves, medidas, col_protocolo="PROTOCOLO", col_solicdata="SOLICDATA"):
    """Retorna as chaves de negócio com mais de um registro.

    A CHAVE é a entidade medida mais a DATA DO REGISTRO — a data do fato ocorrido em
    campo. `PROTOCOLO` e `SOLICDATA` NÃO fazem parte da chave: eles identificam o
    lançamento no sistema, não a medição. Dois protocolos lançados em dias diferentes
    que descrevem a mesma leitura da mesma data são exatamente a duplicidade a
    encontrar, e incluí-los na chave a esconderia.

    Também não se agrupa pelas medidas. Agrupar por elas acharia apenas linhas
    idênticas e deixaria passar o caso pior: mesma chave com valores divergentes.

    A classificação resultante:
      - REENVIO           : mesma leitura lançada duas vezes. Deduplicação segura.
      - CONFLITO DE VALOR : mesma chave com valores diferentes. Exige decisão explícita.
    """
    agg = [
        F.count("*").alias("lancamentos"),
        F.collect_set(F.col(col_protocolo).cast("string")).alias("protocolos"),
        F.sort_array(F.collect_set(F.col(col_solicdata).cast("string"))).alias("datas_lancamento"),
    ]
    for m in medidas:
        agg.append(F.countDistinct(m).alias(f"distintos_{m}"))
        agg.append(F.collect_set(F.col(m).cast("string")).alias(f"valores_{m}"))

    max_distintos = F.greatest(*[F.col(f"distintos_{m}") for m in medidas])

    return (df.groupBy(*chaves).agg(*agg)
              .filter(F.col("lancamentos") > 1)
              .withColumn("classificacao",
                          F.when(max_distintos > 1, F.lit("CONFLITO DE VALOR"))
                           .otherwise(F.lit("REENVIO")))
              .orderBy(F.desc("classificacao"), *chaves))


def resumo_duplicidade(df, chaves, medidas, rotulo):
    """Imprime o placar de duplicidade de uma tabela."""
    d = chave_duplicada(df, chaves, medidas).cache()
    total_chaves = df.select(*chaves).distinct().count()
    reenvio = d.filter(F.col("classificacao") == "REENVIO").count()
    conflito = d.filter(F.col("classificacao") == "CONFLITO DE VALOR").count()
    excedentes = df.count() - total_chaves

    print(f"{rotulo}")
    print(f"  Chave de negócio .................: {chaves}")
    print(f"  Registros na tabela ..............: {df.count()}")
    print(f"  Chaves distintas .................: {total_chaves}")
    print(f"  Lançamentos excedentes ...........: {excedentes}")
    print(f"  Chaves com reenvio ...............: {reenvio}")
    print(f"  Chaves com conflito de valor .....: {conflito}")
    return d


def perfil_completude(df, colunas):
    """Percentual de nulos por coluna."""
    total = df.count()
    linhas = []
    for c in colunas:
        nulos = df.filter(F.col(c).isNull()).count()
        linhas.append({
            "coluna": c,
            "total": total,
            "nulos": nulos,
            "pct_nulos": round(100.0 * nulos / total, 2) if total else None,
        })
    return spark.createDataFrame(linhas).orderBy(F.desc("pct_nulos"))


print("Funções auxiliares definidas.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 2 — Diagnóstico de qualidade
# MAGIC
# MAGIC Verificação executada **sobre a camada Bronze**, antes de qualquer tratamento. Cada
# MAGIC tabela é avaliada nas cinco dimensões de qualidade: completude, consistência, unicidade,
# MAGIC acurácia e presença de outliers.
# MAGIC
# MAGIC As células desta etapa são diagnósticas: não alteram nenhum dado.

# COMMAND ----------

bronze_pluv  = spark.table(f"{CATALOGO}.bronze.leitura_pluviometros")
bronze_bacia = spark.table(f"{CATALOGO}.bronze.medicao_bacias")
bronze_laje  = spark.table(f"{CATALOGO}.bronze.medicao_laje")

print("Volume na Bronze")
print(f"  leitura_pluviometros : {bronze_pluv.count():>6} registros")
print(f"  medicao_bacias       : {bronze_bacia.count():>6} registros")
print(f"  medicao_laje         : {bronze_laje.count():>6} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Consistência — domínio dos campos categóricos
# MAGIC
# MAGIC Verifica se os campos de domínio fechado contêm apenas os valores esperados. Variação de
# MAGIC grafia em um campo categórico quebra junções e duplica membros de dimensão.

# COMMAND ----------

print("STATUS — medicao_bacias")
bronze_bacia.groupBy("STATUS").count().show(truncate=False)

print("STATUS — leitura_pluviometros")
bronze_pluv.groupBy("STATUS").count().show(truncate=False)

print("STATUS — medicao_laje")
bronze_laje.groupBy("STATUS").count().show(truncate=False)

print("UNIDADE — valores distintos por tabela")
for nome, df in [("pluviometros", bronze_pluv), ("bacias", bronze_bacia), ("laje", bronze_laje)]:
    print(f"  {nome}: {[r[0] for r in df.select('UNIDADE').distinct().collect()]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rótulos das estações pluviométricas
# MAGIC
# MAGIC A salina tem cinco estações. Se aparecerem mais de cinco rótulos, há variação de grafia.

# COMMAND ----------

display(
    bronze_pluv.groupBy("ESTACAO")
        .agg(F.count("*").alias("registros"),
             F.min("DATAREGISTRO").alias("primeiro_registro"),
             F.max("DATAREGISTRO").alias("ultimo_registro"))
        .orderBy("ESTACAO")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identificadores das bacias

# COMMAND ----------

display(bronze_bacia.groupBy("BACIA").agg(F.count("*").alias("registros")).orderBy("BACIA"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Consistência — representação de valores ausentes
# MAGIC
# MAGIC Nos arquivos de origem a ausência de valor aparece de duas formas: o caractere `-` e a
# MAGIC célula vazia. Ambas precisam ser convertidas para `NULL`, e a contagem abaixo dimensiona
# MAGIC o problema.

# COMMAND ----------

colunas_numericas_bacia = ["AGUADOCE_COLOCADO", "AGUADOCE_RETIRADO",
                           "SALMOURA_COLOCADO", "SALMOURA_RETIRADO", "SALMOURA_DENSIDADE"]

linhas = []
for c in colunas_numericas_bacia:
    linhas.append({
        "tabela": "medicao_bacias",
        "coluna": c,
        "hifen": bronze_bacia.filter(F.trim(F.col(c)) == "-").count(),
        "vazio_ou_nulo": bronze_bacia.filter(F.col(c).isNull() | (F.trim(F.col(c)) == "")).count(),
        "preenchido": bronze_bacia.filter(
            F.col(c).isNotNull() & (F.trim(F.col(c)) != "-") & (F.trim(F.col(c)) != "")).count(),
    })

linhas.append({
    "tabela": "leitura_pluviometros",
    "coluna": "MILIMETROS",
    "hifen": bronze_pluv.filter(F.trim(F.col("MILIMETROS")) == "-").count(),
    "vazio_ou_nulo": bronze_pluv.filter(
        F.col("MILIMETROS").isNull() | (F.trim(F.col("MILIMETROS")) == "")).count(),
    "preenchido": bronze_pluv.filter(
        F.col("MILIMETROS").isNotNull() & (F.trim(F.col("MILIMETROS")) != "-")
        & (F.trim(F.col("MILIMETROS")) != "")).count(),
})

display(spark.createDataFrame(linhas))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Unicidade — mesma medição lançada mais de uma vez
# MAGIC
# MAGIC ### Definição da chave de negócio
# MAGIC
# MAGIC A chave é a **entidade medida mais a data do registro** — a data em que a medição
# MAGIC ocorreu em campo:
# MAGIC
# MAGIC | Tabela | Chave de negócio |
# MAGIC |---|---|
# MAGIC | `leitura_pluviometros` | (`ESTACAO`, `DATAREGISTRO`) |
# MAGIC | `medicao_bacias` | (`BACIA`, `DATAREGISTRO`) |
# MAGIC | `medicao_laje` | (`CRISTALIZADOR`, `DATAMEDICAO`) |
# MAGIC
# MAGIC **`PROTOCOLO` e `SOLICDATA` ficam fora da chave**, e é isso que faz o teste funcionar.
# MAGIC Eles identificam o *lançamento no sistema*, não a *medição*. O caso típico:
# MAGIC
# MAGIC | Protocolo | SOLICDATA | DATAREGISTRO | Milímetros |
# MAGIC |---|---|---|---|
# MAGIC | 215487 | 01/07/2026 | 01/07/2026 | 12,5 |
# MAGIC | 215479 | 02/07/2026 | 01/07/2026 | 12,5 |
# MAGIC
# MAGIC São duas inclusões no sistema, em dias diferentes, descrevendo **a mesma leitura de
# MAGIC 12,5 mm do dia 01/07**. Existe uma única medição e duas linhas. Se `PROTOCOLO` ou
# MAGIC `SOLICDATA` entrassem na chave, as duas linhas pareceriam registros distintos e a
# MAGIC duplicidade passaria despercebida — que é exatamente o erro a evitar.
# MAGIC
# MAGIC ### Classificação
# MAGIC
# MAGIC | Classificação | Significado | Ação |
# MAGIC |---|---|---|
# MAGIC | `REENVIO` | Mesma leitura lançada mais de uma vez, com valores iguais | Deduplicação segura: manter uma linha |
# MAGIC | `CONFLITO DE VALOR` | Mesma chave com valores diferentes | Exige critério explícito de qual prevalece |
# MAGIC
# MAGIC As colunas `protocolos` e `datas_lancamento` no resultado mostram quais lançamentos
# MAGIC originaram cada duplicidade.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pluviômetros — chave (estação, data de registro)

# COMMAND ----------

dup_pluv = resumo_duplicidade(bronze_pluv, ["ESTACAO", "DATAREGISTRO"], ["MILIMETROS"],
                              "leitura_pluviometros")
display(dup_pluv)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bacias — chave (bacia, data de registro)

# COMMAND ----------

dup_bacia = resumo_duplicidade(bronze_bacia, ["BACIA", "DATAREGISTRO"],
                               ["AGUADOCE_COLOCADO", "SALMOURA_COLOCADO", "SALMOURA_DENSIDADE"],
                               "medicao_bacias")
display(dup_bacia)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Laje — chave (cristalizador, data de medição)

# COMMAND ----------

dup_laje = resumo_duplicidade(bronze_laje, ["CRISTALIZADOR", "DATAMEDICAO"],
                              ["REGUA_1", "REGUA_2", "REGUA_3", "REGUA_4",
                               "REGUA_5", "REGUA_6", "REGUA_7", "REGUA_8"],
                              "medicao_laje")
display(dup_laje)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detalhamento das linhas duplicadas
# MAGIC
# MAGIC Exibe as linhas completas por trás de cada chave duplicada, lado a lado, para que a
# MAGIC decisão de qual registro manter seja tomada com o dado à vista.

# COMMAND ----------

chaves_dup_laje = dup_laje.select("CRISTALIZADOR", "DATAMEDICAO")

display(bronze_laje.join(chaves_dup_laje, ["CRISTALIZADOR", "DATAMEDICAO"])
        .select("CRISTALIZADOR", "DATAMEDICAO", "PROTOCOLO", "SOLICDATA",
                *[f"REGUA_{i}" for i in range(1, 9)])
        .orderBy("CRISTALIZADOR", "DATAMEDICAO", "SOLICDATA"))

# COMMAND ----------

chaves_dup_pluv = dup_pluv.select("ESTACAO", "DATAREGISTRO")

display(bronze_pluv.join(chaves_dup_pluv, ["ESTACAO", "DATAREGISTRO"])
        .select("ESTACAO", "DATAREGISTRO", "PROTOCOLO", "SOLICDATA", "MILIMETROS")
        .orderBy("DATAREGISTRO", "ESTACAO", "SOLICDATA"))

# COMMAND ----------

chaves_dup_bacia = dup_bacia.select("BACIA", "DATAREGISTRO")

display(bronze_bacia.join(chaves_dup_bacia, ["BACIA", "DATAREGISTRO"])
        .select("BACIA", "DATAREGISTRO", "PROTOCOLO", "SOLICDATA",
                "AGUADOCE_COLOCADO", "SALMOURA_COLOCADO", "SALMOURA_DENSIDADE")
        .orderBy("DATAREGISTRO", "BACIA", "SOLICDATA"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Consistência estrutural — regra de emparelhamento das bacias
# MAGIC
# MAGIC A partir de abril de 2026, o formulário de origem passou a registrar **as duas bacias na
# MAGIC mesma solicitação**. Isso cria uma regra verificável: um mesmo protocolo deve cobrir as
# MAGIC duas bacias na mesma data.
# MAGIC
# MAGIC Protocolos que violam a regra indicam registro atribuído à bacia ou à data errada — um
# MAGIC erro que a verificação de duplicidade isolada não encontraria.

# COMMAND ----------

emparelhamento = (bronze_bacia
    .groupBy("PROTOCOLO")
    .agg(F.countDistinct("BACIA").alias("bacias"),
         F.countDistinct("DATAREGISTRO").alias("datas"),
         F.collect_set("BACIA").alias("quais_bacias"),
         F.sort_array(F.collect_set("DATAREGISTRO")).alias("quais_datas"))
    .filter((F.col("bacias") == 2) & (F.col("datas") > 1)))

print("Protocolos que cobrem as duas bacias em datas diferentes (violam a regra):")
display(emparelhamento)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cobertura da série diária das bacias
# MAGIC
# MAGIC A medição é diária. Dias faltantes em uma bacia e presentes na outra reforçam o
# MAGIC diagnóstico de registro atribuído à entidade errada.

# COMMAND ----------

dias_por_bacia = (bronze_bacia
    .groupBy("BACIA")
    .agg(F.countDistinct("DATAREGISTRO").alias("dias_com_registro"),
         F.min("DATAREGISTRO").alias("primeira_data"),
         F.max("DATAREGISTRO").alias("ultima_data")))

display(dias_por_bacia)

datas_b1 = {r[0] for r in bronze_bacia.filter(F.col("BACIA") == "BACIA1").select("DATAREGISTRO").distinct().collect()}
datas_b2 = {r[0] for r in bronze_bacia.filter(F.col("BACIA") == "BACIA2").select("DATAREGISTRO").distinct().collect()}

print(f"Datas presentes na BACIA1 e ausentes na BACIA2: {sorted(datas_b1 - datas_b2)}")
print(f"Datas presentes na BACIA2 e ausentes na BACIA1: {sorted(datas_b2 - datas_b1)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Acurácia — faixa de valores
# MAGIC
# MAGIC Verifica se os valores numéricos fazem sentido no contexto físico do processo.

# COMMAND ----------

perfil = (bronze_bacia.select(
    texto_para_numero("AGUADOCE_COLOCADO", "int").alias("agua_colocado"),
    texto_para_numero("AGUADOCE_RETIRADO", "int").alias("agua_retirado"),
    texto_para_numero("SALMOURA_COLOCADO", "int").alias("salm_colocado"),
    texto_para_numero("SALMOURA_RETIRADO", "int").alias("salm_retirado"),
    texto_para_numero("SALMOURA_DENSIDADE").alias("densidade_be")))

display(perfil.summary("count", "min", "25%", "50%", "75%", "max"))

# COMMAND ----------

reguas = [f"REGUA_{i}" for i in range(1, 9)]

perfil_laje = bronze_laje.select(*[texto_para_numero(c, "int").alias(c.lower()) for c in reguas])
display(perfil_laje.summary("count", "min", "25%", "50%", "75%", "max"))

print("Régua de 1 metro: 100 cm é o limite superior físico da leitura.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Censura no limite da régua
# MAGIC
# MAGIC Quando todas as 8 réguas de uma medição marcam exatamente 100, o valor real é
# MAGIC **maior ou igual a 100** — a régua saturou. Oito leituras idênticas no máximo do
# MAGIC instrumento não são variação natural.

# COMMAND ----------

laje_num = bronze_laje.select(
    texto_para_numero("CRISTALIZADOR", "int").alias("cristalizador"),
    F.col("DATAMEDICAO").alias("data_medicao"),
    *[texto_para_numero(c, "int").alias(c.lower()) for c in reguas])

cols_r = [F.col(c.lower()) for c in reguas]

display(laje_num
    .withColumn("reguas_em_100", sum([F.when(c == 100, 1).otherwise(0) for c in cols_r]))
    .filter(F.col("reguas_em_100") > 0)
    .orderBy(F.desc("reguas_em_100")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outliers nas retiradas das bacias
# MAGIC
# MAGIC Volumes retirados muito acima da faixa usual correspondem a eventos de chuva forte. São
# MAGIC valores legítimos, mas precisam ser identificados para não serem descartados por um
# MAGIC detector automático de outlier.

# COMMAND ----------

display(bronze_bacia
    .select("BACIA", "DATAREGISTRO",
            texto_para_numero("AGUADOCE_RETIRADO", "int").alias("agua_retirado"),
            texto_para_numero("SALMOURA_RETIRADO", "int").alias("salmoura_retirado"),
            texto_para_numero("SALMOURA_DENSIDADE").alias("densidade_be"))
    .filter((F.col("agua_retirado") > 15) | (F.col("salmoura_retirado") > 15))
    .orderBy(F.desc("agua_retirado")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Completude — nulos após a conversão de tipos
# MAGIC
# MAGIC Mede quanto de cada coluna estará efetivamente preenchido na Silver.

# COMMAND ----------

display(perfil_completude(perfil, ["agua_colocado", "agua_retirado", "salm_colocado",
                                   "salm_retirado", "densidade_be"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Completude da densidade por bacia
# MAGIC
# MAGIC A densidade é indispensável para interpretar a taxa de evaporação: sem ela, não se sabe a
# MAGIC que concentração a medição se refere.

# COMMAND ----------

display(bronze_bacia
    .groupBy("BACIA")
    .agg(F.count("*").alias("registros"),
         F.sum(F.when(texto_para_numero("SALMOURA_DENSIDADE").isNotNull(), 1).otherwise(0))
          .alias("com_densidade"))
    .withColumn("pct_com_densidade",
                F.round(100.0 * F.col("com_densidade") / F.col("registros"), 1)))

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 3 — Transformações
# MAGIC
# MAGIC Tratamentos aplicados a partir do diagnóstico acima. Cada bloco declara o que faz e por quê.
# MAGIC
# MAGIC ## Nota sobre a deduplicação
# MAGIC
# MAGIC Em todas as tabelas, a deduplicação distingue dois papéis:
# MAGIC
# MAGIC - **A chave** (`partitionBy`) é a entidade medida mais a data do registro. É ela que
# MAGIC   define o que é a mesma medição.
# MAGIC - **O critério de desempate** (`orderBy`) decide qual das linhas duplicadas permanece.
# MAGIC   `SOLICDATA` é usada aqui, e **apenas aqui**: o lançamento mais recente é tratado como
# MAGIC   correção do anterior.
# MAGIC
# MAGIC A distinção importa. `SOLICDATA` na chave faria cada relançamento parecer uma medição
# MAGIC nova e nenhuma duplicidade seria encontrada; `SOLICDATA` no desempate resolve qual
# MAGIC versão da medição prevalece.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 leitura_pluviometros
# MAGIC
# MAGIC | # | Tratamento | Motivo |
# MAGIC |---|---|---|
# MAGIC | 1 | Filtro `STATUS = 'Solicitação Finalizada'` | Mantém apenas registros homologados |
# MAGIC | 2 | Remoção do ponto final em `ESTACAO` | `PLUVIOMETRO4.` e `PLUVIOMETRO4` são a mesma estação; sem isso a dimensão nasce com 6 membros e a série da estação 4 fica partida |
# MAGIC | 3 | `DATAREGISTRO` para `DATE` | Habilita operações temporais |
# MAGIC | 4 | `MILIMETROS` para `DOUBLE` | Vírgula para ponto; hífen e vazio para `NULL` |
# MAGIC | 5 | Deduplicação por (estação, data) | Critério: registro preenchido prevalece sobre nulo; havendo empate, o lançamento mais recente prevalece, por ser correção do anterior |
# MAGIC | 6 | Descarte de `PROTOCOLO`, `SOLICDATA`, `STATUS`, `UNIDADE`, `OBSERVACOES` | Colunas sem valor analítico |

# COMMAND ----------

pluv = (bronze_pluv
    .filter(F.col("STATUS") == STATUS_HOMOLOGADO)
    .select(
        F.regexp_replace(F.trim(F.col("ESTACAO")), r"\.$", "").alias("estacao"),
        F.to_date(F.col("DATAREGISTRO"), "dd/MM/yyyy").alias("data_registro"),
        texto_para_numero("MILIMETROS").alias("milimetros"),
        F.to_timestamp(F.col("SOLICDATA"), "dd/MM/yyyy HH:mm:ss").alias("_solicdata"),
        F.col("data_ingestao"), F.col("origem"), F.col("arquivo_origem"),
        F.col("data_carga_bronze")))

# Deduplicação: preenchido vence nulo; depois, lançamento mais recente
janela = (Window.partitionBy("estacao", "data_registro")
          .orderBy(F.col("milimetros").isNotNull().desc(),
                   F.col("_solicdata").desc_nulls_last(),
                   F.col("milimetros").desc_nulls_last()))

antes = pluv.count()

silver_pluv = (pluv
    .withColumn("_rn", F.row_number().over(janela))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_solicdata")
    .withColumn("data_carga_silver", F.current_timestamp()))

depois = silver_pluv.count()

(silver_pluv.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOGO}.silver.leitura_pluviometros"))

print(f"Registros na bronze ............: {bronze_pluv.count()}")
print(f"Após filtro de status ..........: {antes}")
print(f"Após deduplicação ..............: {depois}   (removidos: {antes - depois})")
print(f"Estações distintas .............: {silver_pluv.select('estacao').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 medicao_bacias
# MAGIC
# MAGIC | # | Tratamento | Motivo |
# MAGIC |---|---|---|
# MAGIC | 1 | Filtro `STATUS = 'Solicitação Finalizada'` | Mantém apenas registros homologados |
# MAGIC | 2 | Correção de registro atribuído à bacia errada | Ver nota abaixo |
# MAGIC | 3 | Correção de registro atribuído à data errada | Ver nota abaixo |
# MAGIC | 4 | `DATAREGISTRO` para `DATE` | Habilita operações temporais |
# MAGIC | 5 | Volumes para `INT`, densidade para `DOUBLE` | Hífen e vazio para `NULL`; vírgula para ponto |
# MAGIC | 6 | Deduplicação por (bacia, data) | Após as correções, não deve restar duplicata |
# MAGIC | 7 | Descarte de `PROTOCOLO`, `SOLICDATA`, `STATUS`, `UNIDADE`, `OBSERVACOES` | Colunas sem valor analítico |
# MAGIC
# MAGIC ### Nota sobre as duas correções
# MAGIC
# MAGIC O diagnóstico da etapa 2.4 identificou dois protocolos que violam a regra de
# MAGIC emparelhamento, e o cruzamento com a cobertura da série diária permite determinar a
# MAGIC correção de cada um:
# MAGIC
# MAGIC **Protocolo 283327.** A linha registrada como `BACIA1 / 15-01` contém apenas salmoura,
# MAGIC sem água doce e sem densidade — perfil da BACIA2, não da BACIA1. A BACIA1 já tem registro
# MAGIC completo nessa data pelo protocolo 283326, e a BACIA2 não tem registro algum em 15-01.
# MAGIC O registro é da BACIA2 e foi rotulado como BACIA1.
# MAGIC
# MAGIC **Protocolo 326238.** A linha da BACIA2 está datada 12-06, mas o par BACIA1 do mesmo
# MAGIC protocolo é de 11-06, e a BACIA2 não tem registro em 11-06. A data está errada.
# MAGIC
# MAGIC As correções são aplicadas explicitamente, com o protocolo como chave, e ficam
# MAGIC registradas no código — não são deduplicação silenciosa.

# COMMAND ----------

bacia_corrigida = (bronze_bacia
    .filter(F.col("STATUS") == STATUS_HOMOLOGADO)
    # Correção 1: registro da BACIA2 rotulado como BACIA1
    .withColumn("BACIA",
        F.when((F.col("PROTOCOLO") == "283327") & (F.col("DATAREGISTRO") == "15/01/2026"),
               F.lit("BACIA2")).otherwise(F.col("BACIA")))
    # Correção 2: data errada no registro da BACIA2
    .withColumn("DATAREGISTRO",
        F.when((F.col("PROTOCOLO") == "326238") & (F.col("BACIA") == "BACIA2")
               & (F.col("DATAREGISTRO") == "12/06/2026"),
               F.lit("11/06/2026")).otherwise(F.col("DATAREGISTRO"))))

bacia = bacia_corrigida.select(
    F.col("BACIA").alias("bacia"),
    F.to_date(F.col("DATAREGISTRO"), "dd/MM/yyyy").alias("data_registro"),
    texto_para_numero("AGUADOCE_COLOCADO", "int").alias("agua_doce_colocado"),
    texto_para_numero("AGUADOCE_RETIRADO", "int").alias("agua_doce_retirado"),
    texto_para_numero("SALMOURA_COLOCADO", "int").alias("salmoura_colocado"),
    texto_para_numero("SALMOURA_RETIRADO", "int").alias("salmoura_retirado"),
    texto_para_numero("SALMOURA_DENSIDADE").alias("salmoura_densidade"),
    F.to_timestamp(F.col("SOLICDATA"), "dd/MM/yyyy HH:mm:ss").alias("_solicdata"),
    F.col("data_ingestao"), F.col("origem"), F.col("arquivo_origem"),
    F.col("data_carga_bronze"))

janela_b = (Window.partitionBy("bacia", "data_registro")
            .orderBy(F.col("salmoura_densidade").isNotNull().desc(),
                     F.col("_solicdata").desc_nulls_last()))

antes = bacia.count()

silver_bacia = (bacia
    .withColumn("_rn", F.row_number().over(janela_b))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_solicdata")
    .withColumn("data_carga_silver", F.current_timestamp()))

depois = silver_bacia.count()

(silver_bacia.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOGO}.silver.medicao_bacias"))

print(f"Registros na bronze ............: {bronze_bacia.count()}")
print(f"Após filtro de status ..........: {antes}   (removidos: {bronze_bacia.count() - antes})")
print(f"Após correções e deduplicação ..: {depois}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 medicao_laje
# MAGIC
# MAGIC | # | Tratamento | Motivo |
# MAGIC |---|---|---|
# MAGIC | 1 | Filtro `STATUS = 'Solicitação Finalizada'` | Mantém apenas registros homologados |
# MAGIC | 2 | `DATAMEDICAO` para `DATE`, `CRISTALIZADOR` para `INT`, `AREAHECTARES` para `DOUBLE`, réguas para `INT` | Tipagem. `CRISTALIZADOR` vem com zero à esquerda na origem |
# MAGIC | 3 | Deduplicação por (cristalizador, data) | Critério: o lançamento mais recente prevalece, por ser correção do anterior |
# MAGIC | 4 | Cálculo de `media_reguas_cm` e `amplitude_reguas_cm` | A média das 8 réguas é a medida representativa do cristalizador; a amplitude mede a uniformidade da deposição |
# MAGIC | 5 | `flag_censura_regua` | Marca as medições em que todas as réguas atingiram 100 cm, limite físico do instrumento |
# MAGIC | 6 | Descarte de `PROTOCOLO`, `SOLICDATA`, `STATUS`, `UNIDADE`, `OBSERVACOES` | Colunas sem valor analítico |
# MAGIC
# MAGIC **Sobre o desempate:** chave é (`cristalizador`, `data_medicao`); o desempate usa
# MAGIC `SOLICDATA` decrescente, não o número do protocolo. A numeração do protocolo não
# MAGIC acompanha a data da medição — o protocolo 292543 registra fevereiro e o 292552 registra
# MAGIC janeiro. Ordenar por protocolo escolheria o registro errado.

# COMMAND ----------

laje = (bronze_laje
    .filter(F.col("STATUS") == STATUS_HOMOLOGADO)
    .select(
        F.to_date(F.col("DATAMEDICAO"), "dd/MM/yyyy").alias("data_medicao"),
        texto_para_numero("CRISTALIZADOR", "int").alias("cristalizador"),
        texto_para_numero("AREAHECTARES").alias("area_hectares"),
        *[texto_para_numero(c, "int").alias(c.lower()) for c in reguas],
        F.to_timestamp(F.col("SOLICDATA"), "dd/MM/yyyy HH:mm:ss").alias("_solicdata"),
        F.col("data_ingestao"), F.col("origem"), F.col("arquivo_origem"),
        F.col("data_carga_bronze")))

colunas_regua = [F.col(f"regua_{i}") for i in range(1, 9)]

janela_l = (Window.partitionBy("cristalizador", "data_medicao")
            .orderBy(F.col("_solicdata").desc_nulls_last()))

antes = laje.count()

silver_laje = (laje
    .withColumn("_rn", F.row_number().over(janela_l))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_solicdata")
    .withColumn("media_reguas_cm",
                F.round(sum(colunas_regua) / F.lit(8), 3))
    .withColumn("amplitude_reguas_cm",
                F.greatest(*colunas_regua) - F.least(*colunas_regua))
    .withColumn("flag_censura_regua",
                F.least(*colunas_regua) >= 100)
    .withColumn("data_carga_silver", F.current_timestamp()))

depois = silver_laje.count()

(silver_laje.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOGO}.silver.medicao_laje"))

print(f"Registros na bronze ............: {bronze_laje.count()}")
print(f"Após filtro de status ..........: {antes}")
print(f"Após deduplicação ..............: {depois}   (removidos: {antes - depois})")
print(f"Medições com régua censurada ...: {silver_laje.filter('flag_censura_regua').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 4 — Validação pós-tratamento
# MAGIC
# MAGIC Os mesmos testes da etapa 2, agora sobre a Silver. Todos devem retornar vazio ou o
# MAGIC resultado esperado.

# COMMAND ----------

s_pluv  = spark.table(f"{CATALOGO}.silver.leitura_pluviometros")
s_bacia = spark.table(f"{CATALOGO}.silver.medicao_bacias")
s_laje  = spark.table(f"{CATALOGO}.silver.medicao_laje")

falhas = []

# 4.1 Unicidade
for nome, df, chaves in [("leitura_pluviometros", s_pluv,  ["estacao", "data_registro"]),
                         ("medicao_bacias",       s_bacia, ["bacia", "data_registro"]),
                         ("medicao_laje",         s_laje,  ["cristalizador", "data_medicao"])]:
    dup = df.groupBy(*chaves).count().filter(F.col("count") > 1).count()
    ok = dup == 0
    print(f"{'✓' if ok else '✗'} Unicidade de {nome}: {dup} chave(s) duplicada(s)")
    if not ok:
        falhas.append(f"{nome}: {dup} duplicatas")

# 4.2 Estações normalizadas
estacoes = sorted(r[0] for r in s_pluv.select("estacao").distinct().collect())
ok = len(estacoes) == 5
print(f"{'✓' if ok else '✗'} Estações distintas: {len(estacoes)} → {estacoes}")
if not ok:
    falhas.append(f"estações: {len(estacoes)} em vez de 5")

# 4.3 Domínios físicos
checagens = [
    ("densidade fora de 20–30 °Bé",
     s_bacia.filter(F.col("salmoura_densidade").isNotNull()
                    & ~F.col("salmoura_densidade").between(20, 30)).count()),
    ("média de réguas fora de 50–100 cm",
     s_laje.filter(~F.col("media_reguas_cm").between(50, 100)).count()),
    ("precipitação fora de 0–200 mm",
     s_pluv.filter(F.col("milimetros").isNotNull()
                   & ~F.col("milimetros").between(0, 200)).count()),
    ("cristalizador fora de 1–25",
     s_laje.filter(~F.col("cristalizador").between(1, 25)).count()),
]
for descricao, qtd in checagens:
    ok = qtd == 0
    print(f"{'✓' if ok else '✗'} {descricao}: {qtd} registro(s)")
    if not ok:
        falhas.append(f"{descricao}: {qtd}")

# 4.4 Colunas descartadas não devem existir
descartadas = {"protocolo", "solicdata", "status", "unidade", "observacoes"}
for nome, df in [("leitura_pluviometros", s_pluv), ("medicao_bacias", s_bacia), ("medicao_laje", s_laje)]:
    residuais = descartadas & {c.lower() for c in df.columns}
    ok = not residuais
    print(f"{'✓' if ok else '✗'} Colunas descartadas ausentes em {nome}: {sorted(residuais) or 'nenhuma residual'}")
    if not ok:
        falhas.append(f"{nome}: colunas residuais {residuais}")

# 4.5 Datas convertidas
for nome, df, col in [("leitura_pluviometros", s_pluv, "data_registro"),
                      ("medicao_bacias", s_bacia, "data_registro"),
                      ("medicao_laje", s_laje, "data_medicao")]:
    nulos = df.filter(F.col(col).isNull()).count()
    ok = nulos == 0
    print(f"{'✓' if ok else '✗'} Datas convertidas em {nome}: {nulos} nulo(s)")
    if not ok:
        falhas.append(f"{nome}: {nulos} datas nulas")

print("\n" + "=" * 70)
if falhas:
    raise RuntimeError(f"Validação da Silver falhou: {falhas}")
print("Todas as validações de qualidade passaram.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 Confirmação das correções aplicadas nas bacias
# MAGIC
# MAGIC As duas datas corrigidas devem agora aparecer nas duas bacias.

# COMMAND ----------

display(s_bacia
    .filter(F.col("data_registro").isin("2026-01-15", "2026-06-11", "2026-06-12"))
    .orderBy("data_registro", "bacia"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 Cobertura final da série

# COMMAND ----------

display(s_bacia.groupBy("bacia").agg(
    F.count("*").alias("registros"),
    F.countDistinct("data_registro").alias("dias_distintos"),
    F.min("data_registro").alias("primeira_data"),
    F.max("data_registro").alias("ultima_data"),
    F.sum(F.when(F.col("salmoura_densidade").isNotNull(), 1).otherwise(0)).alias("com_densidade")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 5 — Documentação no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mvpdataengineer.silver.leitura_pluviometros IS
# MAGIC   'Camada Silver. Leituras pluviométricas limpas, tipadas e deduplicadas, uma linha por estação e data. Rótulos das estações normalizados. Série registrada por evento: a ausência de linha não equivale a ausência de chuva.';
# MAGIC
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.leitura_pluviometros.estacao IS 'Estação pluviométrica, rótulo normalizado. Domínio: PLUVIOMETRO1 a PLUVIOMETRO5.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.leitura_pluviometros.data_registro IS 'Data da leitura em campo (DATE).';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.leitura_pluviometros.milimetros IS 'Precipitação medida em milímetros (DOUBLE). Domínio esperado: 0 a 200. Nulo quando a leitura não foi registrada.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.leitura_pluviometros.data_carga_silver IS 'Timestamp da carga na camada Silver.';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mvpdataengineer.silver.medicao_bacias IS
# MAGIC   'Camada Silver. Medição diária de evaporação nas bacias, limpa, tipada e deduplicada, uma linha por bacia e dia. Inclui duas correções de registro atribuído à bacia ou à data errada, documentadas no notebook MVP04.';
# MAGIC
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_bacias.bacia IS 'Ponto de medição de evaporação.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_bacias.data_registro IS 'Data da medição em campo (DATE).';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_bacias.agua_doce_colocado IS 'Volume de água doce reposto até a marca, em litros (INT). Corresponde à evaporação do dia. Mutuamente exclusivo com agua_doce_retirado.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_bacias.agua_doce_retirado IS 'Volume de água doce retirado por excedente de chuva, em litros (INT). Mutuamente exclusivo com agua_doce_colocado.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_bacias.salmoura_colocado IS 'Volume de salmoura reposto até a marca, em litros (INT). Corresponde à evaporação do dia. Mutuamente exclusivo com salmoura_retirado.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_bacias.salmoura_retirado IS 'Volume de salmoura retirado por excedente de chuva, em litros (INT). Mutuamente exclusivo com salmoura_colocado.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_bacias.salmoura_densidade IS 'Concentração da salmoura em graus Baumé, °Bé (DOUBLE). Domínio observado: 22,0 a 28,0. Registrada apenas em um dos pontos de medição.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_bacias.data_carga_silver IS 'Timestamp da carga na camada Silver.';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mvpdataengineer.silver.medicao_laje IS
# MAGIC   'Camada Silver. Medição mensal da laje dos cristalizadores, limpa, tipada e deduplicada, uma linha por cristalizador e data. Inclui a média e a amplitude das 8 réguas e a marcação de censura no limite do instrumento.';
# MAGIC
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_laje.cristalizador IS 'Identificador do cristalizador (INT). Domínio: 1 a 25.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_laje.data_medicao IS 'Data da medição em campo (DATE). O intervalo entre medições consecutivas varia de 25 a 35 dias.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_laje.area_hectares IS 'Área do cristalizador em hectares (DOUBLE). Constante por cristalizador: atributo de dimensão.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_laje.media_reguas_cm IS 'Média das 8 réguas, em cm (DOUBLE). Medida representativa da altura livre acima da laje. Domínio esperado: 50 a 100.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_laje.amplitude_reguas_cm IS 'Diferença entre a maior e a menor régua da mesma medição, em cm (INT). Indicador de uniformidade da deposição.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_laje.flag_censura_regua IS 'Verdadeiro quando todas as réguas atingiram 100 cm, limite físico do instrumento. Nesses casos a altura livre real é maior ou igual ao valor registrado.';
# MAGIC COMMENT ON COLUMN mvpdataengineer.silver.medicao_laje.data_carga_silver IS 'Timestamp da carga na camada Silver.';

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 6 — Resumo do tratamento

# COMMAND ----------

resumo_qualidade = [
    {"dimensao": "Consistência", "tabela": "leitura_pluviometros",
     "problema": "PLUVIOMETRO4. e PLUVIOMETRO4 como rótulos distintos da mesma estação",
     "tratamento": "Remoção do ponto final; dimensão passa de 6 para 5 membros"},
    {"dimensao": "Consistência", "tabela": "todas",
     "problema": "Ausência representada ora por hífen, ora por célula vazia",
     "tratamento": "Ambas convertidas para NULL na tipagem"},
    {"dimensao": "Consistência", "tabela": "todas",
     "problema": "Decimal com vírgula impedindo conversão numérica",
     "tratamento": "Substituição por ponto antes do cast"},
    {"dimensao": "Unicidade", "tabela": "medicao_laje",
     "problema": "Mesma medição (cristalizador, data) lançada em mais de um protocolo",
     "tratamento": "Deduplicação pela chave de negócio, mantendo o lançamento mais recente por SOLICDATA"},
    {"dimensao": "Unicidade", "tabela": "leitura_pluviometros",
     "problema": "Mesma leitura (estação, data) lançada em mais de um protocolo, com valores divergentes",
     "tratamento": "Registro preenchido prevalece sobre nulo; empate resolvido pelo lançamento mais recente"},
    {"dimensao": "Unicidade", "tabela": "todas",
     "problema": "PROTOCOLO e SOLICDATA identificam o lançamento, não a medição",
     "tratamento": "Mantidos fora da chave de duplicidade; usados apenas como critério de desempate"},
    {"dimensao": "Acurácia", "tabela": "medicao_bacias",
     "problema": "Registro da BACIA2 rotulado como BACIA1 (protocolo 283327)",
     "tratamento": "Reclassificação explícita, detectada pela regra de emparelhamento de protocolo"},
    {"dimensao": "Acurácia", "tabela": "medicao_bacias",
     "problema": "Registro da BACIA2 com data incorreta (protocolo 326238)",
     "tratamento": "Correção da data para 11/06/2026"},
    {"dimensao": "Acurácia", "tabela": "medicao_laje",
     "problema": "Censura no limite de 100 cm da régua em medições pós-colheita",
     "tratamento": "Marcação com flag_censura_regua; valor real é maior ou igual ao registrado"},
    {"dimensao": "Completude", "tabela": "todas",
     "problema": "Registros não homologados no fluxo de aprovação",
     "tratamento": "Filtro por STATUS = 'Solicitação Finalizada' antes do descarte da coluna"},
    {"dimensao": "Completude", "tabela": "medicao_bacias",
     "problema": "Densidade ausente em um dos pontos de medição",
     "tratamento": "Mantido na Silver; o recorte de escopo é aplicado na camada Gold"},
    {"dimensao": "Relevância", "tabela": "todas",
     "problema": "Colunas sem valor analítico (protocolo, solicdata, status, unidade, observações)",
     "tratamento": "Descartadas após cumprirem sua função de filtro e desempate"},
]

display(spark.createDataFrame(resumo_qualidade))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estrutura final da Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, column_name, data_type, comment
# MAGIC FROM mvpdataengineer.information_schema.columns
# MAGIC WHERE table_schema = 'silver'
# MAGIC ORDER BY table_name, ordinal_position;

# COMMAND ----------

# MAGIC %md
# MAGIC Camada Silver concluída. Prossiga para o notebook **MVP05-Gold**.