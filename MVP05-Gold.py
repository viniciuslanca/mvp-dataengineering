# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MVP05 — Camada Gold
# MAGIC
# MAGIC
# MAGIC ## 📚 Sobre este Notebook
# MAGIC
# MAGIC Este notebook implementa a camada **Gold** a partir dos dados da camada **Silver**, criando visões agregadas prontas para consumo.
# MAGIC
# MAGIC ### Arquitetura Medalhão
# MAGIC
# MAGIC * **📂 Staging**: Dados brutos do Google Drive (notebook MVP02-Download)
# MAGIC * **🥉 Bronze**: Dados brutos com metadados (notebook MVP03-Bronze)
# MAGIC * **🥈 Silver**: Dados limpos e padronizados (notebook MVP04-Silver)
# MAGIC * **🥇 Gold**: Dados agregados e prontos para consumo (este notebook)
# MAGIC
# MAGIC ### 🎯 Objetivo
# MAGIC Criar tabelas no formato **star schema** (fatos e dimensões) a partir dos dados da Silver, usando `CREATE OR REPLACE TABLE AS SELECT`:
# MAGIC
# MAGIC #### Tabelas de Fato
# MAGIC | Tabela | Tipo | Granularidade | Atende |
# MAGIC |---|---|---|---|
# MAGIC | `fato_precipitacao_diaria` | Fato | Estação × dia | Perguntas 1, 2 e 3 |
# MAGIC | `fato_evaporacao_diaria` | Fato | Fluido × dia | Variável climática |
# MAGIC | `fato_laje_medicao` | Fato | Cristalizador × medição | Perguntas 1 e 2 |
# MAGIC | `fato_analise_mensal` | Fato analítico | Mês | **Perguntas 1 e 2** |
# MAGIC | `fato_chuva_anual` | Fato analítico | Ano | **Pergunta 3** |
# MAGIC
# MAGIC #### Tabelas de Dimensão 
# MAGIC | Tabela | Tipo | Granularidade | Atende |
# MAGIC |---|---|---|---|
# MAGIC | `dim_tempo` | Dimensão | Dia | Fatos diários |
# MAGIC | `dim_mes` | Dimensão | Mês | `fato_analise_mensal` |
# MAGIC | `dim_cristalizador` | Dimensão | Cristalizador | — |
# MAGIC | `dim_estacao` | Dimensão | Estação | — |
# MAGIC | `dim_fluido` | Dimensão | Fluido | — |
# MAGIC
# MAGIC
# MAGIC ## Perguntas de negócio
# MAGIC
# MAGIC 1. Qual é a relação quantitativa entre a precipitação acumulada e a variação da espessura da laje?
# MAGIC 2. Qual regime de precipitação é mais danoso: grande volume em poucos eventos ou chuvas frequentes de volume reduzido?
# MAGIC 3. O período analisado representa um ano de chuva típico, seco ou atípico frente à série histórica?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Etapa 1 — Parâmetros
# MAGIC
# MAGIC **Dois recortes temporais distintos**:
# MAGIC
# MAGIC - A **análise cruzada** (perguntas 1 e 2) vai de janeiro a agosto de 2026, limitada pela
# MAGIC   série de medição da laje.
# MAGIC - A **série pluviométrica** é carregada por inteiro, desde 2014, porque a pergunta 3
# MAGIC   depende exatamente do histórico que as outras fontes não têm.

# COMMAND ----------

CATALOGO = "mvpdataengineer"

RECORTE_INICIO = "2026-01-01"
RECORTE_FIM    = "2026-08-31"
SERIE_INICIO   = "2014-01-01"
SERIE_FIM      = "2026-12-31"

# Bacia com leitura de densidade. A outra é excluída do escopo: sem densidade, a taxa de evaporação medida não é interpretável.
BACIA_ESCOPO = "BACIA1"

# Estação com suspeita de descalibração — ver justificativa na criação de dim_estacao.
ESTACAO_SUSPEITA = "PLUVIOMETRO5"

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql("USE SCHEMA gold")

for k, v in [("Catálogo", CATALOGO), ("Recorte da análise", f"{RECORTE_INICIO} a {RECORTE_FIM}"),
             ("Série pluviométrica", f"{SERIE_INICIO} a {SERIE_FIM}"),
             ("Bacia no escopo", BACIA_ESCOPO), ("Estação suspeita", ESTACAO_SUSPEITA)]:
    print(f"{k:.<24} {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥇 Etapa 2 — Dimensões

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 dim_tempo
# MAGIC
# MAGIC Calendário-base na granularidade dia, gerado por sequência e não extraído dos fatos. Assim nenhuma data fica de fora por ausência de medição.
# MAGIC
# MAGIC `estacao_chuvosa` e `no_recorte_analise` transformam filtros recorrentes em atributos de dimensão, evitando que a mesma condição seja reescrita em cada consulta e divirja.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.dim_tempo AS
WITH dias AS (
  SELECT explode(sequence(DATE'{SERIE_INICIO}', DATE'{SERIE_FIM}', INTERVAL 1 DAY)) AS data
)
SELECT
  data,
  YEAR(data)                                              AS ano,
  MONTH(data)                                             AS mes,
  DAY(data)                                               AS dia,
  DATE_FORMAT(data, 'yyyy-MM')                            AS ano_mes,
  CASE MONTH(data)
    WHEN 1 THEN 'Janeiro'  WHEN 2 THEN 'Fevereiro' WHEN 3  THEN 'Março'
    WHEN 4 THEN 'Abril'    WHEN 5 THEN 'Maio'      WHEN 6  THEN 'Junho'
    WHEN 7 THEN 'Julho'    WHEN 8 THEN 'Agosto'    WHEN 9  THEN 'Setembro'
    WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
  END                                                     AS nome_mes,
  CAST(CEIL(MONTH(data) / 3.0) AS INT)                    AS trimestre,
  (MONTH(data) BETWEEN 2 AND 5)                           AS estacao_chuvosa,
  (data BETWEEN DATE'{RECORTE_INICIO}' AND DATE'{RECORTE_FIM}') AS no_recorte_analise,
  CURRENT_TIMESTAMP()                                     AS data_carga_gold
FROM dias
""")

print(f"dim_tempo: {spark.table(f'{CATALOGO}.gold.dim_tempo').count()} dias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 dim_mes
# MAGIC
# MAGIC Redução conformada de `dim_tempo` na granularidade mês, para atender `fato_analise_mensal`.
# MAGIC
# MAGIC **É derivada de `dim_tempo`, não construída em paralelo.** Isso garante que `nome_mes`, `trimestre` e `estacao_chuvosa` signifiquem exatamente a mesma coisa nos dois níveis.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.dim_mes AS
SELECT
  ano_mes,
  MAX(ano)                AS ano,
  MAX(mes)                AS mes,
  MAX(nome_mes)           AS nome_mes,
  MAX(trimestre)          AS trimestre,
  MAX(estacao_chuvosa)    AS estacao_chuvosa,
  MAX(no_recorte_analise) AS no_recorte_analise,
  COUNT(*)                AS dias_no_mes,
  CURRENT_TIMESTAMP()     AS data_carga_gold
FROM {CATALOGO}.gold.dim_tempo
GROUP BY ano_mes
""")

print(f"dim_mes: {spark.table(f'{CATALOGO}.gold.dim_mes').count()} meses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 dim_cristalizador
# MAGIC
# MAGIC A área é constante por cristalizador na tabela origem. Por isso migra para a dimensão em vez de ser repetida em cada linha de fato.
# MAGIC
# MAGIC `faixa_area` classifica por tercil e permite verificar se cristalizadores de tamanhos diferentes respondem de forma distinta à chuva. Não responderem também é resultado: indica que o efeito é do fenômeno, não da geometria do tanque.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.dim_cristalizador AS
WITH base AS (
  SELECT cristalizador, MAX(area_hectares) AS area_hectares
  FROM {CATALOGO}.silver.medicao_laje
  GROUP BY cristalizador
),
tercis AS (
  SELECT
    PERCENTILE_APPROX(area_hectares, 0.3333) AS t1,
    PERCENTILE_APPROX(area_hectares, 0.6667) AS t2
  FROM base
)
SELECT
  b.cristalizador,
  ROUND(b.area_hectares, 2)          AS area_hectares,
  ROUND(b.area_hectares * 10000, 2)  AS area_m2,
  CASE WHEN b.area_hectares <= t.t1 THEN 'PEQUENO'
       WHEN b.area_hectares <= t.t2 THEN 'MEDIO'
       ELSE 'GRANDE' END             AS faixa_area,
  CURRENT_TIMESTAMP()                AS data_carga_gold
FROM base b CROSS JOIN tercis t
""")

display(spark.table(f"{CATALOGO}.gold.dim_cristalizador").orderBy("cristalizador"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 dim_estacao
# MAGIC
# MAGIC ##### Por que existe `flag_suspeita_calibracao`
# MAGIC
# MAGIC Entre 2014 e 2023 a estação 5 registrou, em média, 1,14 vez a média das outras quatro.
# MAGIC A partir de 2024 essa razão cai para cerca de 0,6 e permanece nesse patamar por três anos consecutivos. 
# MAGIC Dez anos oscilando em torno de 1,1 e três anos consecutivos em 0,6 não é variabilidade espacial de chuva é comportamento de equipamento. 
# MAGIC Pode ter havido obstrução, vazamento ou mudança de procedimento de leitura.
# MAGIC
# MAGIC Marcar a estação como atributo de dimensão, em vez de descartá-la ou ignorar o problema, permite gerar a análise com e sem ela e **mostrar** o impacto da escolha.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.dim_estacao AS
SELECT DISTINCT
  estacao,
  (estacao = '{ESTACAO_SUSPEITA}') AS flag_suspeita_calibracao,
  CASE WHEN estacao = '{ESTACAO_SUSPEITA}'
       THEN 'Razão em relação à média das demais estações cai de 1,14 (2014-2023) para cerca de 0,6 a partir de 2024, por três anos consecutivos. Indica descalibração, obstrução ou mudança de procedimento de leitura.'
       ELSE NULL END AS observacao_qualidade,
  CURRENT_TIMESTAMP() AS data_carga_gold
FROM {CATALOGO}.silver.leitura_pluviometros
""")

display(spark.table(f"{CATALOGO}.gold.dim_estacao").orderBy("estacao"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 dim_fluido
# MAGIC
# MAGIC Os atributos físicos da bacia, diâmetro e área do espelho d'água, **não existem na origem**: são conhecimento do processo, declarados aqui. 
# MAGIC A dimensão é o lugar certo para eles, e é ela que permite converter litros em milímetros sem espalhar a constante pelo código.
# MAGIC
# MAGIC Bacia circular de 1 metro de diâmetro: área = π × (0,5)² = 0,7854 m². Logo, 1 litro corresponde a 1,2732 mm de lâmina.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.dim_fluido AS
SELECT 'AGUA_DOCE' AS fluido, 'Água doce' AS descricao,
       CAST(1.0 AS DECIMAL(4,2)) AS diametro_m,
       CAST(ROUND(PI() * POWER(0.5, 2), 4) AS DECIMAL(6,4)) AS area_m2,
       FALSE AS mede_densidade,
       'Referência de evaporação sem influência de salinidade' AS papel,
       CURRENT_TIMESTAMP() AS data_carga_gold
UNION ALL
SELECT 'SALMOURA', 'Salmoura',
       CAST(1.0 AS DECIMAL(4,2)),
       CAST(ROUND(PI() * POWER(0.5, 2), 4) AS DECIMAL(6,4)),
       TRUE,
       'Evaporação em condição de produção, com densidade medida em graus Baumé',
       CURRENT_TIMESTAMP()
""")

display(spark.table(f"{CATALOGO}.gold.dim_fluido"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥇 Etapa 3 — Fatos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 fato_precipitacao_diaria
# MAGIC
# MAGIC Granularidade: estação × dia. **Série completa, sem recorte** — é o insumo da pergunta 3.
# MAGIC
# MAGIC **O grão diário é obrigatório.** Agregar direto para mês descartaria a contagem de dias com chuva, que é metade da pergunta 2.
# MAGIC
# MAGIC **A ausência de linha não significa ausência de chuva.** A série é registrada por evento. O pluviômetro é lido porque choveu, não em agenda fixa. Nenhum preenchimento com zero é aplicado aqui nem em nenhuma tabela derivada.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.fato_precipitacao_diaria AS
SELECT
  estacao,
  data_registro                   AS data,
  CAST(milimetros AS DECIMAL(6,2)) AS milimetros,
  (milimetros > 0)                AS houve_chuva,
  CURRENT_TIMESTAMP()             AS data_carga_gold
FROM {CATALOGO}.silver.leitura_pluviometros
WHERE milimetros IS NOT NULL
""")

print(f"fato_precipitacao_diaria: {spark.table(f'{CATALOGO}.gold.fato_precipitacao_diaria').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 fato_evaporacao_diaria
# MAGIC
# MAGIC Granularidade: fluido × dia. Recorte de 2026, apenas a bacia com leitura de densidade.
# MAGIC
# MAGIC ### Consolidação em medida com sinal
# MAGIC
# MAGIC Na origem, "colocado" e "retirado" ocupam colunas distintas, mas nunca aparecem preenchidos no mesmo dia. São estados mutuamente exclusivos do mesmo evento. Aqui viram uma única medida: **positivo é reposição, ou seja, evaporação; negativo é retirada, ou seja, excedente de chuva.** O estado fica registrado em `tipo_evento`.
# MAGIC
# MAGIC ### Por que esta tabela é a variável climática do modelo
# MAGIC
# MAGIC A evaporação depende de radiação solar, vento, nebulosidade e umidade. **Nenhuma dessas variáveis é medida na salina.** A lâmina evaporada é o indicador composto de todas elas. Taxa alta significa sol forte e céu limpo e taxa baixa ou negativa significa céu encoberto e chuva.
# MAGIC Sem ela, a variação da laje seria explicada apenas pela chuva, e a análise confundiria dois efeitos distintos: sal dissolvido pela água e sal que deixou de se formar por falta de energia evaporativa.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.fato_evaporacao_diaria AS
WITH bruto AS (
  SELECT 'AGUA_DOCE' AS fluido, data_registro AS data,
         agua_doce_colocado AS colocado, agua_doce_retirado AS retirado,
         CAST(NULL AS DOUBLE) AS densidade_be
  FROM {CATALOGO}.silver.medicao_bacias
  WHERE bacia = '{BACIA_ESCOPO}'
    AND data_registro BETWEEN DATE'{RECORTE_INICIO}' AND DATE'{RECORTE_FIM}'
  UNION ALL
  SELECT 'SALMOURA', data_registro,
         salmoura_colocado, salmoura_retirado, salmoura_densidade
  FROM {CATALOGO}.silver.medicao_bacias
  WHERE bacia = '{BACIA_ESCOPO}'
    AND data_registro BETWEEN DATE'{RECORTE_INICIO}' AND DATE'{RECORTE_FIM}'
),
com_area AS (
  SELECT b.*, d.area_m2
  FROM bruto b JOIN {CATALOGO}.gold.dim_fluido d USING (fluido)
)
SELECT
  fluido,
  data,
  CASE WHEN retirado IS NOT NULL THEN 'RETIRADA'
       WHEN colocado IS NOT NULL THEN 'REPOSICAO'
       ELSE 'SEM_LEITURA' END                                        AS tipo_evento,
  CASE WHEN colocado IS NULL AND retirado IS NULL THEN NULL
       ELSE CAST(COALESCE(colocado, 0) - COALESCE(retirado, 0) AS DECIMAL(7,2))
  END                                                                AS volume_litros,
  CASE WHEN colocado IS NULL AND retirado IS NULL THEN NULL
       ELSE CAST(ROUND((COALESCE(colocado, 0) - COALESCE(retirado, 0)) / area_m2, 2) AS DECIMAL(7,2))
  END                                                                AS lamina_mm,
  CAST(densidade_be AS DECIMAL(4,1))                                 AS densidade_be,
  CAST(ROUND(densidade_be - LAG(densidade_be) OVER (
         PARTITION BY fluido ORDER BY data), 2) AS DECIMAL(4,2))     AS variacao_densidade_be,
  CURRENT_TIMESTAMP()                                                AS data_carga_gold
FROM com_area
""")

display(spark.sql(f"""
SELECT fluido, tipo_evento, COUNT(*) AS dias,
       ROUND(AVG(lamina_mm), 2) AS lamina_media_mm
FROM {CATALOGO}.gold.fato_evaporacao_diaria
GROUP BY fluido, tipo_evento ORDER BY fluido, tipo_evento
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 fato_laje_medicao
# MAGIC
# MAGIC Granularidade: cristalizador × data de medição.
# MAGIC
# MAGIC #### As medidas derivadas são o que torna a tabela utilizável
# MAGIC
# MAGIC A leitura bruta registra um **nível**; a análise precisa da **variação entre níveis**.
# MAGIC
# MAGIC Três derivações sustentam tudo o que vem depois:
# MAGIC
# MAGIC **`variacao_cm`** — diferença para a medição anterior do mesmo cristalizador.
# MAGIC **`dias_corridos`** — o intervalo entre medições vai de 25 a 35 dias. Comparar variações brutas sem normalizar introduz até 40% de erro.
# MAGIC **`flag_colheita`** — queda superior a 5 cm na espessura indica remoção da camada de sal, não efeito de chuva. Esses períodos não podem entrar na análise.
# MAGIC
# MAGIC #### A medida é a espessura, não a leitura
# MAGIC
# MAGIC A régua de 1 metro registra a **altura livre acima da laje**; a espessura da camada de sal é o complemento, `100 − leitura`, e a conversão já vem feita da Silver. Todas as medidas desta tabela usam a espessura, de modo que o sinal é o intuitivo: **positivo é sal ganho, negativo é sal perdido**.
# MAGIC
# MAGIC #### Espessura zero é cristalizador recém-colhido
# MAGIC
# MAGIC `flag_laje_zerada` marca as medições em que a espessura é zero. Não é falha nem limite de instrumento: zero é o piso físico da medida — o fundo do cristalizador — e significa que a colheita acabou de ocorrer.
# MAGIC
# MAGIC `flag_base_zerada` marca a medição **seguinte** a uma dessas. Sua variação corresponde a um ciclo de formação iniciado do zero, o que é contexto útil para interpretar o crescimento do período.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.fato_laje_medicao AS
WITH base AS (
  SELECT cristalizador, data_medicao,
         CAST(espessura_laje_cm AS DECIMAL(6,3)) AS espessura_laje_cm,
         CAST(media_reguas_cm   AS DECIMAL(6,3)) AS leitura_regua_cm,
         amplitude_reguas_cm,
         flag_laje_zerada
  FROM {CATALOGO}.silver.medicao_laje
  WHERE data_medicao BETWEEN DATE'{RECORTE_INICIO}' AND DATE'{RECORTE_FIM}'
),
com_lag AS (
  SELECT *,
    LAG(espessura_laje_cm) OVER w AS espessura_anterior_cm,
    LAG(data_medicao)      OVER w AS data_medicao_anterior,
    LAG(flag_laje_zerada)  OVER w AS flag_base_zerada
  FROM base
  WINDOW w AS (PARTITION BY cristalizador ORDER BY data_medicao)
)
SELECT
  cristalizador,
  data_medicao,
  data_medicao_anterior,
  DATEDIFF(data_medicao, data_medicao_anterior)                      AS dias_corridos,
  espessura_laje_cm,
  espessura_anterior_cm,
  leitura_regua_cm,
  amplitude_reguas_cm,
  CAST(espessura_laje_cm - espessura_anterior_cm AS DECIMAL(6,3))    AS variacao_cm,
  CAST(ROUND((espessura_laje_cm - espessura_anterior_cm)
       / NULLIF(DATEDIFF(data_medicao, data_medicao_anterior), 0), 4)
       AS DECIMAL(7,4))                                              AS variacao_cm_dia,
  ((espessura_laje_cm - espessura_anterior_cm) < -5)                 AS flag_colheita,
  CASE WHEN (espessura_laje_cm - espessura_anterior_cm) >= -5
       THEN CAST(espessura_laje_cm - espessura_anterior_cm AS DECIMAL(6,3))
       END                                                           AS crescimento_laje_cm,
  flag_laje_zerada,
  COALESCE(flag_base_zerada, FALSE)                                  AS flag_base_zerada,
  CURRENT_TIMESTAMP()                                                AS data_carga_gold
FROM com_lag
""")

display(spark.sql(f"""
SELECT DATE_FORMAT(data_medicao, 'yyyy-MM') AS ano_mes,
       COUNT(*)                                        AS medicoes,
       SUM(CASE WHEN flag_colheita THEN 1 ELSE 0 END)  AS colheitas,
       SUM(CASE WHEN flag_laje_zerada THEN 1 ELSE 0 END) AS laje_zerada,
       ROUND(AVG(espessura_laje_cm), 2)                AS espessura_media_cm,
       ROUND(AVG(CASE WHEN variacao_cm >= -5 THEN variacao_cm END), 2) AS crescimento_medio_cm
FROM {CATALOGO}.gold.fato_laje_medicao
GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 4 — Tabelas analíticas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 fato_analise_mensal — responde as perguntas 1 e 2
# MAGIC
# MAGIC #### A janela de atribuição
# MAGIC
# MAGIC A chuva é acumulada **entre a medição anterior e a atual**, não no mês-calendário. As medições caem entre os dias 25 e 31 e variam por cristalizador; usar mês-calendário atribuiria chuva à janela errada. O mesmo vale para a evaporação.
# MAGIC
# MAGIC #### Por que agregar por mês
# MAGIC
# MAGIC Existem cerca de 160 pares cristalizador × janela sem colheita. Tratá-los como 160 observações independentes é **pseudo-replicação**: os 25 cristalizadores compartilham a mesma chuva e variam quase em uníssono, com desvio médio entre eles de 0,35 cm. São 25 medidas do mesmo fenômeno, não 25 observações.
# MAGIC
# MAGIC A tabela entrega o dado **já agregado por mês** justamente para que a análise não caia nessa armadilha por descuido. O n efetivo é o número de meses.
# MAGIC
# MAGIC #### As quatro medidas de chuva
# MAGIC
# MAGIC A pergunta 2 exige separar regimes: volume, frequência, intensidade e evento extremo. Uma medida só não responde.
# MAGIC
# MAGIC `dias_lidos_janela` mede o **esforço de observação**. O pluviômetro é lido porque choveu, não em agenda fixa, e o número de dias lidos correlaciona 0,82 com o volume do mês. Volume e frequência estão parcialmente acoplados pelo processo de coleta, e essa coluna existe para expor o acoplamento em vez de escondê-lo.
# MAGIC
# MAGIC ### As duas flags de cobertura
# MAGIC
# MAGIC `cobertura_pluviometrica_ok` evita o erro mais perigoso do conjunto. A série pluviométrica termina em 27 de julho e a janela de agosto não tem leitura alguma. Se a soma retornasse zero, agosto entraria nos gráficos como "mês sem chuva" — e é o ponto de maior alavancagem na ponta inferior da regressão. Sem cobertura, as medidas de chuva são **nulas**, não zero.
# MAGIC
# MAGIC `cobertura_bacia_ok` marca o oposto: a série de bacias vai até setembro e **cobre a janela de agosto**. Nela há zero dias de retirada em 45 dias e a maior evaporação da série, o que dá evidência independente de que agosto foi efetivamente seco.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.fato_analise_mensal AS
WITH limites AS (
  SELECT (SELECT MAX(data) FROM {CATALOGO}.gold.fato_precipitacao_diaria) AS max_pluv,
         (SELECT MAX(data) FROM {CATALOGO}.gold.fato_evaporacao_diaria)   AS max_bacia
),
chuva_dia AS (
  SELECT data, AVG(milimetros) AS chuva_mm
  FROM {CATALOGO}.gold.fato_precipitacao_diaria
  GROUP BY data
),
leituras_dia AS (
  SELECT DISTINCT data FROM {CATALOGO}.gold.fato_precipitacao_diaria
),
evap_dia AS (
  SELECT data,
         MAX(CASE WHEN fluido = 'AGUA_DOCE' THEN lamina_mm END) AS evap_agua_mm,
         MAX(CASE WHEN fluido = 'SALMOURA'  THEN lamina_mm END) AS evap_salmoura_mm,
         MAX(CASE WHEN tipo_evento = 'RETIRADA' THEN 1 ELSE 0 END) AS teve_retirada
  FROM {CATALOGO}.gold.fato_evaporacao_diaria
  GROUP BY data
),
janelas AS (
  SELECT
    l.cristalizador, l.data_medicao, l.data_medicao_anterior,
    l.dias_corridos, l.variacao_cm, l.crescimento_laje_cm,
    l.flag_base_zerada,
    lim.max_pluv, lim.max_bacia,
    SUM(c.chuva_mm)                                            AS chuva_volume_mm,
    COUNT(CASE WHEN c.chuva_mm > 0 THEN 1 END)                 AS chuva_dias_registrados,
    MAX(c.chuva_mm)                                            AS chuva_max_dia_mm
  FROM {CATALOGO}.gold.fato_laje_medicao l
  CROSS JOIN limites lim
  LEFT JOIN chuva_dia c
         ON c.data >  l.data_medicao_anterior
        AND c.data <= l.data_medicao
  WHERE l.data_medicao_anterior IS NOT NULL
    AND NOT l.flag_colheita
  GROUP BY l.cristalizador, l.data_medicao, l.data_medicao_anterior,
           l.dias_corridos, l.variacao_cm, l.crescimento_laje_cm,
           l.flag_base_zerada, lim.max_pluv, lim.max_bacia
),
com_leituras AS (
  SELECT j.*, COUNT(ld.data) AS dias_lidos_janela
  FROM janelas j
  LEFT JOIN leituras_dia ld
         ON ld.data >  j.data_medicao_anterior
        AND ld.data <= j.data_medicao
  GROUP BY ALL
),
com_evap AS (
  SELECT ce.*,
         SUM(e.evap_agua_mm)     AS evap_agua_total_mm,
         SUM(e.evap_salmoura_mm) AS evap_salmoura_total_mm,
         SUM(e.teve_retirada)    AS dias_retirada_janela,
         COUNT(e.data)           AS dias_bacia_janela
  FROM com_leituras ce
  LEFT JOIN evap_dia e
         ON e.data >  ce.data_medicao_anterior
        AND e.data <= ce.data_medicao
  GROUP BY ALL
),
por_janela AS (
  SELECT *,
    (data_medicao <= max_pluv)  AS cobertura_pluviometrica_ok,
    (data_medicao <= max_bacia) AS cobertura_bacia_ok
  FROM com_evap
)
SELECT
  DATE_FORMAT(data_medicao, 'yyyy-MM')                        AS ano_mes,
  COUNT(*)                                                    AS cristalizadores,
  CAST(ROUND(AVG(variacao_cm), 3)    AS DECIMAL(6,3))         AS variacao_media_cm,
  CAST(ROUND(STDDEV(variacao_cm), 3) AS DECIMAL(6,3))         AS variacao_desvio_cm,
  CAST(ROUND(AVG(dias_corridos), 1)  AS DECIMAL(4,1))         AS dias_corridos_medio,
  SUM(CASE WHEN flag_base_zerada THEN 1 ELSE 0 END)           AS medicoes_pos_colheita,

  -- Medidas de chuva: nulas quando a janela não tem cobertura pluviométrica
  CASE WHEN MAX(cobertura_pluviometrica_ok)
       THEN CAST(ROUND(AVG(chuva_volume_mm), 1) AS DECIMAL(6,1)) END        AS chuva_volume_mm,
  CASE WHEN MAX(cobertura_pluviometrica_ok)
       THEN CAST(ROUND(AVG(chuva_dias_registrados), 1) AS DECIMAL(4,1)) END AS chuva_dias_registrados,
  CASE WHEN MAX(cobertura_pluviometrica_ok)
       THEN CAST(ROUND(AVG(chuva_volume_mm) / NULLIF(AVG(chuva_dias_registrados), 0), 2)
            AS DECIMAL(6,2)) END                                            AS chuva_intensidade_mm_dia,
  CASE WHEN MAX(cobertura_pluviometrica_ok)
       THEN CAST(ROUND(AVG(chuva_max_dia_mm), 1) AS DECIMAL(6,1)) END       AS chuva_max_dia_mm,
  CASE WHEN MAX(cobertura_pluviometrica_ok)
       THEN CAST(ROUND(AVG(dias_lidos_janela), 1) AS DECIMAL(4,1)) END      AS dias_lidos_janela,

  -- Medidas climáticas da bacia
  CAST(ROUND(AVG(evap_agua_total_mm)     / NULLIF(AVG(dias_corridos), 0), 2) AS DECIMAL(6,2)) AS evaporacao_agua_mm_dia,
  CAST(ROUND(AVG(evap_salmoura_total_mm) / NULLIF(AVG(dias_corridos), 0), 2) AS DECIMAL(6,2)) AS evaporacao_salmoura_mm_dia,
  CAST(ROUND(AVG(dias_retirada_janela), 1) AS DECIMAL(4,1))                  AS dias_retirada_janela,

  MAX(cobertura_pluviometrica_ok)                             AS cobertura_pluviometrica_ok,
  MAX(cobertura_bacia_ok)                                     AS cobertura_bacia_ok,
  CURRENT_TIMESTAMP()                                         AS data_carga_gold
FROM por_janela
GROUP BY DATE_FORMAT(data_medicao, 'yyyy-MM')
""")

display(spark.table(f"{CATALOGO}.gold.fato_analise_mensal").orderBy("ano_mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 fato_chuva_anual — responde a pergunta 3
# MAGIC
# MAGIC #### Comparar o mesmo período do ano
# MAGIC
# MAGIC A série pluviométrica de 2026 termina em 27 de julho. Confrontar sete meses de 2026 com doze meses dos anos anteriores faria 2026 parecer artificialmente seco. Por isso a medida de referência é o **acumulado de janeiro a julho**, comparável entre todos os anos.
# MAGIC
# MAGIC #### Com e sem a estação suspeita
# MAGIC
# MAGIC A estação 5 subestima a precipitação a partir de 2024, justamente nos anos que mais interessam à comparação. A tabela traz as duas versões do acumulado para que a escolha fique visível em vez de embutida.
# MAGIC
# MAGIC #### Classificação
# MAGIC
# MAGIC A comparação é contra a média histórica de 2014 a 2025, excluindo 2026. Um ano é classificado como `SECO` ou `CHUVOSO` quando se afasta mais de um desvio-padrão da média, e `TIPICO` caso contrário.
# MAGIC
# MAGIC #### Por que materializar
# MAGIC
# MAGIC A classificação exige a média histórica excluindo o ano corrente e a janela comparável de janeiro a julho. 
# MAGIC Não é um `GROUP BY` óbvio, e materializar garante que todos os gráficos usem o mesmo critério.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.fato_chuva_anual AS
WITH por_estacao AS (
  SELECT YEAR(f.data) AS ano, f.estacao, e.flag_suspeita_calibracao,
         SUM(CASE WHEN MONTH(f.data) <= 7 THEN f.milimetros ELSE 0 END) AS mm_jan_jul,
         SUM(f.milimetros)                                              AS mm_ano
  FROM {CATALOGO}.gold.fato_precipitacao_diaria f
  JOIN {CATALOGO}.gold.dim_estacao e USING (estacao)
  GROUP BY YEAR(f.data), f.estacao, e.flag_suspeita_calibracao
),
por_ano AS (
  SELECT ano,
         AVG(mm_jan_jul)                                                     AS total_mm_jan_jul,
         AVG(CASE WHEN NOT flag_suspeita_calibracao THEN mm_jan_jul END)     AS total_mm_jan_jul_sem_suspeita,
         AVG(mm_ano)                                                         AS total_mm_ano
  FROM por_estacao GROUP BY ano
),
dias AS (
  SELECT YEAR(data) AS ano,
         COUNT(DISTINCT CASE WHEN houve_chuva THEN data END) AS dias_com_chuva,
         MAX(milimetros)                                     AS maior_evento_mm
  FROM {CATALOGO}.gold.fato_precipitacao_diaria GROUP BY YEAR(data)
),
historico AS (
  SELECT AVG(total_mm_jan_jul_sem_suspeita)    AS media_hist,
         STDDEV(total_mm_jan_jul_sem_suspeita) AS desvio_hist,
         COUNT(*)                              AS anos_hist
  FROM por_ano WHERE ano BETWEEN 2014 AND 2025
),
-- Percentil por junção agregada: subconsulta correlacionada no SELECT
-- tem suporte limitado no Spark.
percentis AS (
  SELECT a.ano,
         100.0 * COUNT(h.ano) / MAX(n.anos) AS percentil
  FROM por_ano a
  CROSS JOIN (SELECT COUNT(*) AS anos FROM por_ano WHERE ano BETWEEN 2014 AND 2025) n
  LEFT JOIN por_ano h
         ON h.ano BETWEEN 2014 AND 2025
        AND h.total_mm_jan_jul_sem_suspeita < a.total_mm_jan_jul_sem_suspeita
  GROUP BY a.ano
)
SELECT
  a.ano,
  CAST(ROUND(a.total_mm_jan_jul, 1)              AS DECIMAL(7,1)) AS total_mm_jan_jul,
  CAST(ROUND(a.total_mm_jan_jul_sem_suspeita, 1) AS DECIMAL(7,1)) AS total_mm_jan_jul_sem_suspeita,
  CAST(ROUND(a.total_mm_ano, 1)                  AS DECIMAL(7,1)) AS total_mm_ano,
  d.dias_com_chuva,
  CAST(ROUND(d.maior_evento_mm, 1) AS DECIMAL(6,1))               AS maior_evento_mm,
  CAST(ROUND(h.media_hist, 1)      AS DECIMAL(7,1))               AS media_historica_mm,
  CAST(ROUND(h.desvio_hist, 1)     AS DECIMAL(7,1))               AS desvio_historico_mm,
  CAST(ROUND(100.0 * (a.total_mm_jan_jul_sem_suspeita / h.media_hist - 1), 1)
       AS DECIMAL(6,1))                                           AS desvio_vs_media_pct,
  CAST(ROUND(pc.percentil, 0) AS DECIMAL(5,1))                    AS percentil_na_serie,
  CASE
    WHEN a.total_mm_jan_jul_sem_suspeita < h.media_hist - h.desvio_hist THEN 'SECO'
    WHEN a.total_mm_jan_jul_sem_suspeita > h.media_hist + h.desvio_hist THEN 'CHUVOSO'
    ELSE 'TIPICO'
  END                                                             AS classificacao,
  (a.ano = 2026)                                                  AS ano_do_estudo,
  CURRENT_TIMESTAMP()                                             AS data_carga_gold
FROM por_ano a
JOIN dias d USING (ano)
JOIN percentis pc ON pc.ano = a.ano
CROSS JOIN historico h
""")

display(spark.table(f"{CATALOGO}.gold.fato_chuva_anual").orderBy("ano"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 fato_chuva_mensal_historico — a pergunta 3 na granularidade mês
# MAGIC
# MAGIC A pergunta 3 comparada apenas por total anual responde "quanto choveu", mas não **quando**. Esta tabela compara, mês a mês, a precipitação de 2026 com a média histórica do mesmo mês entre 2014 e 2025.
# MAGIC
# MAGIC A distinção importa: um ano pode ter volume total dentro da média e ainda assim ter a estação chuvosa deslocada, o que muda completamente o efeito sobre a produção, já que a laje reage à chuva do período, não à do ano.
# MAGIC
# MAGIC A faixa entre o mínimo e o máximo históricos permite desenhar o envelope de variação esperada e posicionar 2026 dentro dele.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.fato_chuva_mensal_historico AS
WITH por_ano_mes AS (
  SELECT YEAR(f.data) AS ano, MONTH(f.data) AS mes,
         AVG(total_estacao) AS mm
  FROM (
    SELECT f.data, f.estacao, SUM(f.milimetros) AS total_estacao
    FROM {CATALOGO}.gold.fato_precipitacao_diaria f
    JOIN {CATALOGO}.gold.dim_estacao e USING (estacao)
    WHERE NOT e.flag_suspeita_calibracao
    GROUP BY f.data, f.estacao
  ) f
  GROUP BY YEAR(f.data), MONTH(f.data)
),
por_estacao_mes AS (
  SELECT YEAR(f.data) AS ano, MONTH(f.data) AS mes, f.estacao, SUM(f.milimetros) AS mm
  FROM {CATALOGO}.gold.fato_precipitacao_diaria f
  JOIN {CATALOGO}.gold.dim_estacao e USING (estacao)
  WHERE NOT e.flag_suspeita_calibracao
  GROUP BY YEAR(f.data), MONTH(f.data), f.estacao
),
mensal AS (
  SELECT ano, mes, AVG(mm) AS mm FROM por_estacao_mes GROUP BY ano, mes
),
historico AS (
  SELECT mes,
         AVG(mm)    AS media_historica_mm,
         STDDEV(mm) AS desvio_historico_mm,
         MIN(mm)    AS minimo_historico_mm,
         MAX(mm)    AS maximo_historico_mm,
         COUNT(*)   AS anos_observados
  FROM mensal WHERE ano BETWEEN 2014 AND 2025
  GROUP BY mes
),
ano_estudo AS (
  SELECT mes, mm AS chuva_2026_mm FROM mensal WHERE ano = 2026
)
SELECT
  h.mes,
  CASE h.mes
    WHEN 1 THEN 'Janeiro'  WHEN 2 THEN 'Fevereiro' WHEN 3  THEN 'Março'
    WHEN 4 THEN 'Abril'    WHEN 5 THEN 'Maio'      WHEN 6  THEN 'Junho'
    WHEN 7 THEN 'Julho'    WHEN 8 THEN 'Agosto'    WHEN 9  THEN 'Setembro'
    WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
  END                                                           AS nome_mes,
  CAST(ROUND(a.chuva_2026_mm, 1)      AS DECIMAL(6,1))          AS chuva_2026_mm,
  CAST(ROUND(h.media_historica_mm, 1) AS DECIMAL(6,1))          AS media_historica_mm,
  CAST(ROUND(h.desvio_historico_mm, 1) AS DECIMAL(6,1))         AS desvio_historico_mm,
  CAST(ROUND(h.minimo_historico_mm, 1) AS DECIMAL(6,1))         AS minimo_historico_mm,
  CAST(ROUND(h.maximo_historico_mm, 1) AS DECIMAL(6,1))         AS maximo_historico_mm,
  h.anos_observados,
  CAST(ROUND(100.0 * (a.chuva_2026_mm / NULLIF(h.media_historica_mm, 0) - 1), 0)
       AS DECIMAL(6,1))                                         AS desvio_vs_media_pct,
  CASE
    WHEN a.chuva_2026_mm IS NULL THEN 'SEM DADO'
    WHEN a.chuva_2026_mm < h.media_historica_mm - h.desvio_historico_mm THEN 'ABAIXO'
    WHEN a.chuva_2026_mm > h.media_historica_mm + h.desvio_historico_mm THEN 'ACIMA'
    ELSE 'DENTRO DA FAIXA'
  END                                                           AS situacao,
  (h.mes BETWEEN 2 AND 5)                                       AS estacao_chuvosa,
  CURRENT_TIMESTAMP()                                           AS data_carga_gold
FROM historico h
LEFT JOIN ano_estudo a USING (mes)
ORDER BY h.mes
""")

display(spark.table(f"{CATALOGO}.gold.fato_chuva_mensal_historico").orderBy("mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 5 — Visões para os gráficos
# MAGIC
# MAGIC As análises serão feitas com os gráficos nativos do Databricks, sobre o resultado de uma consulta. 
# MAGIC
# MAGIC ## O problema de escala
# MAGIC
# MAGIC As métricas mensais têm amplitudes que diferem em até 24 vezes:
# MAGIC
# MAGIC | Métrica | Amplitude |
# MAGIC |---|---|
# MAGIC | Crescimento da laje | −1,6 a +3,0 cm |
# MAGIC | Volume de chuva | 0 a 110 mm |
# MAGIC | Dias com chuva | 0 a 14 |
# MAGIC | Intensidade | 4 a 14 mm/dia |
# MAGIC | Evaporação | 4 a 16 mm/dia |
# MAGIC
# MAGIC Num gráfico de eixo único, a variação da laje vira uma linha reta colada no zero enquanto a chuva ocupa toda a altura. Por isso cada visão traz **o valor real e o valor normalizado** em escala 0 a 100.
# MAGIC
# MAGIC Use o valor real quando a série for sozinha no gráfico ou quando puder configurar eixo secundário; use o normalizado quando quiser sobrepor séries de unidades diferentes para comparar o **formato** das curvas.
# MAGIC
# MAGIC A normalização é min-max sobre os meses disponíveis: o menor valor da série vira 0, o maior vira 100. Ela preserva a forma da curva e descarta a magnitude, que é exatamente o que se quer ao comparar comportamentos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Pergunta 1 — chuva e variação da laje, mês a mês
# MAGIC
# MAGIC **Gráfico principal:**. Barras para `chuva_volume_mm` e linha para `crescimento_laje_cm`, com eixo secundário. 
# MAGIC
# MAGIC **Gráfico complementar:** dispersão de `chuva_volume_mm` contra `crescimento_laje_cm`, que mostra a relação sem a dimensão temporal.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOGO}.gold.vw_grafico_p1 AS
SELECT
  ano_mes,
  m.nome_mes,
  variacao_media_cm                 AS crescimento_laje_cm,
  chuva_volume_mm,
  evaporacao_agua_mm_dia,
  variacao_desvio_cm,
  cristalizadores,
  -- Normalização min-max para sobreposição em eixo único
  CAST(ROUND(100.0 * (variacao_media_cm - MIN(variacao_media_cm) OVER ())
       / NULLIF(MAX(variacao_media_cm) OVER () - MIN(variacao_media_cm) OVER (), 0), 1)
       AS DECIMAL(5,1))             AS laje_norm,
  CAST(ROUND(100.0 * (chuva_volume_mm - MIN(chuva_volume_mm) OVER ())
       / NULLIF(MAX(chuva_volume_mm) OVER () - MIN(chuva_volume_mm) OVER (), 0), 1)
       AS DECIMAL(5,1))             AS chuva_norm,
  CAST(ROUND(100.0 * (evaporacao_agua_mm_dia - MIN(evaporacao_agua_mm_dia) OVER ())
       / NULLIF(MAX(evaporacao_agua_mm_dia) OVER () - MIN(evaporacao_agua_mm_dia) OVER (), 0), 1)
       AS DECIMAL(5,1))             AS evaporacao_norm,
  cobertura_pluviometrica_ok
FROM {CATALOGO}.gold.fato_analise_mensal f
JOIN {CATALOGO}.gold.dim_mes m USING (ano_mes)
""")

display(spark.sql(f"SELECT * FROM {CATALOGO}.gold.vw_grafico_p1 ORDER BY ano_mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Pergunta 2 — qual regime acompanha a variação da laje
# MAGIC
# MAGIC **Gráfico principal:** linhas. Eixo X `ano_mes`, e quatro séries normalizadas: `volume_norm`, `frequencia_norm`, `intensidade_norm` e `laje_invertida_norm`.
# MAGIC
# MAGIC A série da laje entra **invertida** porque a relação é negativa: mais chuva, menos sal.
# MAGIC Invertida, o regime que de fato explica a variação é o que acompanha visualmente a curva da laje. 

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOGO}.gold.vw_grafico_p2 AS
WITH base AS (
  SELECT ano_mes, variacao_media_cm, chuva_volume_mm,
         chuva_dias_registrados, chuva_intensidade_mm_dia,
         chuva_max_dia_mm, dias_lidos_janela, cobertura_pluviometrica_ok
  FROM {CATALOGO}.gold.fato_analise_mensal
  WHERE cobertura_pluviometrica_ok
)
SELECT
  ano_mes,
  variacao_media_cm        AS crescimento_laje_cm,
  chuva_volume_mm,
  chuva_dias_registrados,
  chuva_intensidade_mm_dia,
  chuva_max_dia_mm,
  dias_lidos_janela,
  -- Laje invertida: a relação com a chuva é negativa, então inverter alinha as curvas
  CAST(ROUND(100.0 * (MAX(variacao_media_cm) OVER () - variacao_media_cm)
       / NULLIF(MAX(variacao_media_cm) OVER () - MIN(variacao_media_cm) OVER (), 0), 1)
       AS DECIMAL(5,1))    AS laje_invertida_norm,
  CAST(ROUND(100.0 * (chuva_volume_mm - MIN(chuva_volume_mm) OVER ())
       / NULLIF(MAX(chuva_volume_mm) OVER () - MIN(chuva_volume_mm) OVER (), 0), 1)
       AS DECIMAL(5,1))    AS volume_norm,
  CAST(ROUND(100.0 * (chuva_dias_registrados - MIN(chuva_dias_registrados) OVER ())
       / NULLIF(MAX(chuva_dias_registrados) OVER () - MIN(chuva_dias_registrados) OVER (), 0), 1)
       AS DECIMAL(5,1))    AS frequencia_norm,
  CAST(ROUND(100.0 * (chuva_intensidade_mm_dia - MIN(chuva_intensidade_mm_dia) OVER ())
       / NULLIF(MAX(chuva_intensidade_mm_dia) OVER () - MIN(chuva_intensidade_mm_dia) OVER (), 0), 1)
       AS DECIMAL(5,1))    AS intensidade_norm
FROM base
""")

display(spark.sql(f"SELECT * FROM {CATALOGO}.gold.vw_grafico_p2 ORDER BY ano_mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Pergunta 3 — perfil sazonal de 2026 contra o histórico
# MAGIC
# MAGIC **Gráfico principal:** combinado. Barras para `chuva_2026_mm`, linha para `media_historica_mm`. 
# MAGIC
# MAGIC As colunas `minimo_historico_mm` e `maximo_historico_mm` desenham o envelope de variação esperada, e mostram se 2026 saiu da faixa ou apenas se moveu dentro dela.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOGO}.gold.vw_grafico_p3_sazonal AS
SELECT mes, nome_mes, chuva_2026_mm, media_historica_mm,
       minimo_historico_mm, maximo_historico_mm,
       desvio_vs_media_pct, situacao, estacao_chuvosa
FROM {CATALOGO}.gold.fato_chuva_mensal_historico
WHERE chuva_2026_mm IS NOT NULL
""")

display(spark.sql(f"SELECT * FROM {CATALOGO}.gold.vw_grafico_p3_sazonal ORDER BY mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Pergunta 3 — total anual comparado
# MAGIC
# MAGIC **Gráfico principal:** barras de `chuva_jan_jul_mm` por ano, com linha de `media_historica_mm`. 
# MAGIC A coluna `ano_do_estudo` permite destacar 2026 na cor.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOGO}.gold.vw_grafico_p3_anual AS
SELECT ano,
       total_mm_jan_jul_sem_suspeita AS chuva_jan_jul_mm,
       media_historica_mm,
       media_historica_mm - desvio_historico_mm AS limite_inferior_mm,
       media_historica_mm + desvio_historico_mm AS limite_superior_mm,
       desvio_vs_media_pct, percentil_na_serie, classificacao, ano_do_estudo
FROM {CATALOGO}.gold.fato_chuva_anual
""")

display(spark.sql(f"SELECT * FROM {CATALOGO}.gold.vw_grafico_p3_anual ORDER BY ano"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 Formato longo — uma métrica por linha
# MAGIC
# MAGIC Algumas comparações ficam mais simples com os dados empilhados: uma linha por mês e métrica, em vez de uma coluna por métrica. 
# MAGIC Nesse formato, um único gráfico de linhas com `metrica` como agrupamento mostra todas as séries, e filtrar por `pergunta` troca o
# MAGIC conteúdo do gráfico sem reescrever a consulta.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOGO}.gold.vw_grafico_mensal_long AS
WITH norm AS (
  SELECT ano_mes, 'Crescimento da laje' AS metrica, 'cm' AS unidade, 'P1 e P2' AS pergunta,
         variacao_media_cm AS valor FROM {CATALOGO}.gold.fato_analise_mensal
  UNION ALL
  SELECT ano_mes, 'Volume de chuva', 'mm', 'P1 e P2', chuva_volume_mm
  FROM {CATALOGO}.gold.fato_analise_mensal
  UNION ALL
  SELECT ano_mes, 'Dias com chuva', 'dias', 'P2', chuva_dias_registrados
  FROM {CATALOGO}.gold.fato_analise_mensal
  UNION ALL
  SELECT ano_mes, 'Intensidade da chuva', 'mm/dia', 'P2', chuva_intensidade_mm_dia
  FROM {CATALOGO}.gold.fato_analise_mensal
  UNION ALL
  SELECT ano_mes, 'Evaporação (clima)', 'mm/dia', 'P1', evaporacao_agua_mm_dia
  FROM {CATALOGO}.gold.fato_analise_mensal
)
SELECT ano_mes, pergunta, metrica, unidade,
       CAST(valor AS DECIMAL(8,2)) AS valor,
       CAST(ROUND(100.0 * (valor - MIN(valor) OVER (PARTITION BY metrica))
            / NULLIF(MAX(valor) OVER (PARTITION BY metrica)
                   - MIN(valor) OVER (PARTITION BY metrica), 0), 1)
            AS DECIMAL(5,1)) AS valor_normalizado
FROM norm
WHERE valor IS NOT NULL
""")

display(spark.sql(f"""
  SELECT * FROM {CATALOGO}.gold.vw_grafico_mensal_long ORDER BY metrica, ano_mes
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 6 — Chaves e restrições
# MAGIC
# MAGIC ## Chaves informativas
# MAGIC
# MAGIC Chaves primárias e estrangeiras no Unity Catalog são **informativas**, mas documentam o modelo e fazem o Catalog Explorer renderizar o diagrama de relacionamento automaticamente.

# COMMAND ----------

ddl_chaves = [
    # Dimensões
    "ALTER TABLE {c}.gold.dim_tempo         ALTER COLUMN data          SET NOT NULL",
    "ALTER TABLE {c}.gold.dim_tempo         ADD CONSTRAINT pk_dim_tempo         PRIMARY KEY (data)",
    "ALTER TABLE {c}.gold.dim_mes           ALTER COLUMN ano_mes       SET NOT NULL",
    "ALTER TABLE {c}.gold.dim_mes           ADD CONSTRAINT pk_dim_mes           PRIMARY KEY (ano_mes)",
    "ALTER TABLE {c}.gold.dim_cristalizador ALTER COLUMN cristalizador SET NOT NULL",
    "ALTER TABLE {c}.gold.dim_cristalizador ADD CONSTRAINT pk_dim_cristalizador PRIMARY KEY (cristalizador)",
    "ALTER TABLE {c}.gold.dim_estacao       ALTER COLUMN estacao       SET NOT NULL",
    "ALTER TABLE {c}.gold.dim_estacao       ADD CONSTRAINT pk_dim_estacao       PRIMARY KEY (estacao)",
    "ALTER TABLE {c}.gold.dim_fluido        ALTER COLUMN fluido        SET NOT NULL",
    "ALTER TABLE {c}.gold.dim_fluido        ADD CONSTRAINT pk_dim_fluido        PRIMARY KEY (fluido)",
    # Fatos analíticos
    "ALTER TABLE {c}.gold.fato_analise_mensal ALTER COLUMN ano_mes SET NOT NULL",
    "ALTER TABLE {c}.gold.fato_analise_mensal ADD CONSTRAINT pk_analise_mensal PRIMARY KEY (ano_mes)",
    "ALTER TABLE {c}.gold.fato_chuva_anual    ALTER COLUMN ano     SET NOT NULL",
    "ALTER TABLE {c}.gold.fato_chuva_anual    ADD CONSTRAINT pk_chuva_anual    PRIMARY KEY (ano)",
    # Estrangeiras
    "ALTER TABLE {c}.gold.fato_precipitacao_diaria ADD CONSTRAINT fk_precip_tempo   FOREIGN KEY (data)          REFERENCES {c}.gold.dim_tempo",
    "ALTER TABLE {c}.gold.fato_precipitacao_diaria ADD CONSTRAINT fk_precip_estacao FOREIGN KEY (estacao)       REFERENCES {c}.gold.dim_estacao",
    "ALTER TABLE {c}.gold.fato_evaporacao_diaria   ADD CONSTRAINT fk_evap_tempo     FOREIGN KEY (data)          REFERENCES {c}.gold.dim_tempo",
    "ALTER TABLE {c}.gold.fato_evaporacao_diaria   ADD CONSTRAINT fk_evap_fluido    FOREIGN KEY (fluido)        REFERENCES {c}.gold.dim_fluido",
    "ALTER TABLE {c}.gold.fato_laje_medicao        ADD CONSTRAINT fk_laje_tempo     FOREIGN KEY (data_medicao)  REFERENCES {c}.gold.dim_tempo",
    "ALTER TABLE {c}.gold.fato_laje_medicao        ADD CONSTRAINT fk_laje_crist     FOREIGN KEY (cristalizador) REFERENCES {c}.gold.dim_cristalizador",
    "ALTER TABLE {c}.gold.fato_analise_mensal      ADD CONSTRAINT fk_analise_mes    FOREIGN KEY (ano_mes)       REFERENCES {c}.gold.dim_mes",
]

for ddl in ddl_chaves:
    sql = ddl.format(c=CATALOGO)
    try:
        spark.sql(sql)
        print(f"✓ {sql.split('CONSTRAINT')[-1].split('REFERENCES')[0].strip() if 'CONSTRAINT' in sql else 'NOT NULL'}")
    except Exception as e:
        msg = str(e).split("\n")[0]
        print(f"· já existente ou não aplicável: {msg[:90]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restrições de qualidade
# MAGIC
# MAGIC Diferente das chaves, `CHECK` é **imposta na gravação**. Isso transforma os domínios de valores levantados no diagnóstico de qualidade em teste executável a cada carga: se uma futura ingestão trouxer densidade fora da faixa física ou régua acima do limite do instrumento, a gravação falha em vez de propagar o erro até a análise.

# COMMAND ----------

ddl_checks = [
    "ALTER TABLE {c}.gold.fato_precipitacao_diaria ADD CONSTRAINT chk_chuva      CHECK (milimetros BETWEEN 0 AND 200)",
    "ALTER TABLE {c}.gold.fato_evaporacao_diaria   ADD CONSTRAINT chk_densidade  CHECK (densidade_be IS NULL OR densidade_be BETWEEN 20 AND 30)",
    "ALTER TABLE {c}.gold.fato_evaporacao_diaria   ADD CONSTRAINT chk_lamina     CHECK (lamina_mm BETWEEN -60 AND 60)",
    "ALTER TABLE {c}.gold.fato_laje_medicao        ADD CONSTRAINT chk_espessura  CHECK (espessura_laje_cm BETWEEN 0 AND 50)",
    "ALTER TABLE {c}.gold.fato_laje_medicao        ADD CONSTRAINT chk_amplitude  CHECK (amplitude_reguas_cm BETWEEN 0 AND 10)",
    "ALTER TABLE {c}.gold.fato_laje_medicao        ADD CONSTRAINT chk_intervalo  CHECK (dias_corridos IS NULL OR dias_corridos BETWEEN 20 AND 40)",
    "ALTER TABLE {c}.gold.dim_cristalizador        ADD CONSTRAINT chk_area       CHECK (area_hectares BETWEEN 1 AND 50)",
]

for ddl in ddl_checks:
    sql = ddl.format(c=CATALOGO)
    try:
        spark.sql(sql)
        print(f"✓ {sql.split('CONSTRAINT')[1].split('CHECK')[0].strip()}")
    except Exception as e:
        print(f"· {sql.split('CONSTRAINT')[1].split('CHECK')[0].strip()}: {str(e).split(chr(10))[0][:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 7 — Validação da Gold

# COMMAND ----------

from pyspark.sql import functions as F

falhas = []

def checar(descricao, condicao, detalhe=""):
    print(f"{'✓' if condicao else '✗'} {descricao}{(' — ' + str(detalhe)) if detalhe else ''}")
    if not condicao:
        falhas.append(descricao)

t = lambda nome: spark.table(f"{CATALOGO}.gold.{nome}")

# Cardinalidade das dimensões
checar("dim_cristalizador tem 25 linhas", t("dim_cristalizador").count() == 25, t("dim_cristalizador").count())
checar("dim_estacao tem 5 linhas",        t("dim_estacao").count() == 5,        t("dim_estacao").count())
checar("dim_fluido tem 2 linhas",         t("dim_fluido").count() == 2,         t("dim_fluido").count())

# Unicidade das chaves
for tabela, chave in [("dim_tempo", "data"), ("dim_mes", "ano_mes"),
                      ("dim_cristalizador", "cristalizador"), ("dim_estacao", "estacao"),
                      ("dim_fluido", "fluido"), ("fato_analise_mensal", "ano_mes"),
                      ("fato_chuva_anual", "ano")]:
    dup = t(tabela).groupBy(chave).count().filter("count > 1").count()
    checar(f"{tabela}.{chave} é única", dup == 0, f"{dup} duplicata(s)")

# Integridade referencial
orfas = t("fato_precipitacao_diaria").join(t("dim_tempo"), "data", "left_anti").count()
checar("fato_precipitacao_diaria sem data órfã em dim_tempo", orfas == 0, orfas)

orfas = t("fato_laje_medicao").join(t("dim_cristalizador"), "cristalizador", "left_anti").count()
checar("fato_laje_medicao sem cristalizador órfão", orfas == 0, orfas)

orfas = t("fato_analise_mensal").join(t("dim_mes"), "ano_mes", "left_anti").count()
checar("fato_analise_mensal sem mês órfão em dim_mes", orfas == 0, orfas)

# Coerência conformada entre dim_tempo e dim_mes
divergentes = (t("dim_tempo").select("ano_mes", "nome_mes", "trimestre").distinct()
               .join(t("dim_mes").select("ano_mes",
                        F.col("nome_mes").alias("nome_mes_m"),
                        F.col("trimestre").alias("trimestre_m")), "ano_mes")
               .filter("nome_mes <> nome_mes_m OR trimestre <> trimestre_m").count())
checar("dim_mes é conformada com dim_tempo", divergentes == 0, divergentes)

# Regra central: agosto não pode ter chuva zero
ago = t("fato_analise_mensal").filter("ano_mes = '2026-08'").collect()
if ago:
    r = ago[0]
    checar("agosto tem chuva NULA, não zero",
           r["chuva_volume_mm"] is None,
           f"chuva_volume_mm = {r['chuva_volume_mm']}, cobertura_pluv = {r['cobertura_pluviometrica_ok']}")
    checar("agosto tem cobertura de bacia", bool(r["cobertura_bacia_ok"]),
           f"evaporação = {r['evaporacao_agua_mm_dia']} mm/dia, dias de retirada = {r['dias_retirada_janela']}")

# Colheitas fora da análise mensal
colheitas = t("fato_laje_medicao").filter("flag_colheita").count()
checar("colheitas identificadas e excluídas da análise", colheitas > 0, f"{colheitas} evento(s)")

# Coerência da conversão leitura -> espessura
incoerentes = t("fato_laje_medicao").filter(
    "abs(espessura_laje_cm - (100 - leitura_regua_cm)) > 0.001").count()
checar("espessura é o complemento exato da leitura", incoerentes == 0, incoerentes)

fora = t("fato_laje_medicao").filter("espessura_laje_cm < 0").count()
checar("nenhuma espessura negativa", fora == 0, fora)

print("\n" + "=" * 70)
if falhas:
    raise RuntimeError(f"Validação da Gold falhou: {falhas}")
print("Todas as validações da Gold passaram.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 8 — Respostas preliminares
# MAGIC
# MAGIC Consultas que extraem de cada tabela o número que responde a pergunta correspondente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — relação entre chuva e variação da laje

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano_mes, cristalizadores,
# MAGIC        variacao_media_cm AS crescimento_laje_cm, variacao_desvio_cm, dias_corridos_medio,
# MAGIC        chuva_volume_mm, evaporacao_agua_mm_dia,
# MAGIC        cobertura_pluviometrica_ok, cobertura_bacia_ok
# MAGIC FROM mvpdataengineer.gold.fato_analise_mensal
# MAGIC ORDER BY ano_mes;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — qual regime explica melhor a variação
# MAGIC
# MAGIC Correlação de cada regime candidato com a variação da laje. A evaporação entra como referência: sendo o mecanismo direto de formação do sal, espera-se que seja o preditor mais forte.
# MAGIC
# MAGIC Chuva e evaporação são fortemente colineares. Mês chuvoso é mês nublado. 
# MAGIC As correlações abaixo devem ser lidas uma a uma, não como um modelo conjunto.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Volume de chuva'        AS regime, ROUND(CORR(chuva_volume_mm,           variacao_media_cm), 3) AS correlacao,
# MAGIC   ROUND(POWER(CORR(chuva_volume_mm, variacao_media_cm), 2), 3) AS r2
# MAGIC FROM mvpdataengineer.gold.fato_analise_mensal WHERE cobertura_pluviometrica_ok
# MAGIC UNION ALL
# MAGIC SELECT 'Dias com chuva',      ROUND(CORR(chuva_dias_registrados,   variacao_media_cm), 3),
# MAGIC        ROUND(POWER(CORR(chuva_dias_registrados, variacao_media_cm), 2), 3)
# MAGIC FROM mvpdataengineer.gold.fato_analise_mensal WHERE cobertura_pluviometrica_ok
# MAGIC UNION ALL
# MAGIC SELECT 'Intensidade da chuva', ROUND(CORR(chuva_intensidade_mm_dia, variacao_media_cm), 3),
# MAGIC        ROUND(POWER(CORR(chuva_intensidade_mm_dia, variacao_media_cm), 2), 3)
# MAGIC FROM mvpdataengineer.gold.fato_analise_mensal WHERE cobertura_pluviometrica_ok
# MAGIC UNION ALL
# MAGIC SELECT 'Evaporação (clima)',   ROUND(CORR(evaporacao_agua_mm_dia,   variacao_media_cm), 3),
# MAGIC        ROUND(POWER(CORR(evaporacao_agua_mm_dia, variacao_media_cm), 2), 3)
# MAGIC FROM mvpdataengineer.gold.fato_analise_mensal
# MAGIC ORDER BY r2 DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência do acoplamento entre volume e frequência
# MAGIC
# MAGIC O pluviômetro é lido porque choveu. Se o esforço de observação acompanhar o volume, as medidas de volume e frequência não são independentes e isso precisa constar da análise.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(CORR(chuva_volume_mm, dias_lidos_janela), 3)      AS corr_volume_x_esforco_leitura,
# MAGIC   ROUND(CORR(chuva_volume_mm, chuva_dias_registrados), 3) AS corr_volume_x_frequencia,
# MAGIC   ROUND(CORR(chuva_volume_mm, evaporacao_agua_mm_dia), 3) AS corr_volume_x_evaporacao
# MAGIC FROM mvpdataengineer.gold.fato_analise_mensal
# MAGIC WHERE cobertura_pluviometrica_ok;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Volume × esforço de leitura' AS par,
# MAGIC        ROUND(CORR(chuva_volume_mm, dias_lidos_janela), 3)      AS correlacao
# MAGIC FROM mvpdataengineer.gold.fato_analise_mensal WHERE cobertura_pluviometrica_ok
# MAGIC UNION ALL
# MAGIC SELECT 'Volume × frequência de chuva',
# MAGIC        ROUND(CORR(chuva_volume_mm, chuva_dias_registrados), 3)
# MAGIC FROM mvpdataengineer.gold.fato_analise_mensal WHERE cobertura_pluviometrica_ok
# MAGIC UNION ALL
# MAGIC SELECT 'Volume × evaporação',
# MAGIC        ROUND(CORR(chuva_volume_mm, evaporacao_agua_mm_dia), 3)
# MAGIC FROM mvpdataengineer.gold.fato_analise_mensal WHERE cobertura_pluviometrica_ok
# MAGIC ORDER BY ABS(correlacao) DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — 2026 foi típico, seco ou atípico

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, total_mm_jan_jul_sem_suspeita AS chuva_jan_jul_mm,
# MAGIC        media_historica_mm, desvio_vs_media_pct, percentil_na_serie, classificacao
# MAGIC FROM mvpdataengineer.gold.fato_chuva_anual
# MAGIC ORDER BY ano;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mvpdataengineer.gold.vw_grafico_p3_sazonal ORDER BY mes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Impacto de incluir ou não a estação suspeita

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano,
# MAGIC        total_mm_jan_jul              AS com_estacao_suspeita,
# MAGIC        total_mm_jan_jul_sem_suspeita AS sem_estacao_suspeita,
# MAGIC        ROUND(total_mm_jan_jul - total_mm_jan_jul_sem_suspeita, 1) AS diferenca_mm
# MAGIC FROM mvpdataengineer.gold.fato_chuva_anual
# MAGIC WHERE ano >= 2022 ORDER BY ano;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contexto — efeito da salinidade sobre a evaporação
# MAGIC
# MAGIC Não responde nenhuma das três perguntas, mas quantifica o mecanismo de produção. Qual a razão entre a evaporação da salmoura e a da água doce, medidas lado a lado sob o mesmo clima?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_FORMAT(data, 'yyyy-MM') AS ano_mes,
# MAGIC   ROUND(AVG(CASE WHEN fluido = 'AGUA_DOCE' THEN lamina_mm END), 2) AS agua_doce_mm_dia,
# MAGIC   ROUND(AVG(CASE WHEN fluido = 'SALMOURA'  THEN lamina_mm END), 2) AS salmoura_mm_dia,
# MAGIC   ROUND(AVG(CASE WHEN fluido = 'SALMOURA'  THEN lamina_mm END)
# MAGIC       / NULLIF(AVG(CASE WHEN fluido = 'AGUA_DOCE' THEN lamina_mm END), 0), 3) AS razao_salinidade
# MAGIC FROM mvpdataengineer.gold.fato_evaporacao_diaria
# MAGIC WHERE tipo_evento = 'REPOSICAO'
# MAGIC GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mvpdataengineer.gold.vw_grafico_mensal_long
# MAGIC WHERE pergunta LIKE '%P2%'
# MAGIC ORDER BY metrica, ano_mes

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 9 — Documentação no Unity Catalog

# COMMAND ----------

comentarios_tabela = {
    "dim_tempo": "Dimensão temporal conformada, granularidade de um dia, cobrindo 2014 a 2026. Atende os fatos de granularidade diária. Inclui atributos de filtro (estação chuvosa e recorte da análise) para que condições recorrentes não sejam reescritas em cada consulta.",
    "dim_mes": "Dimensão temporal no granularidade mês, derivada de dim_tempo para garantir conformidade dos atributos entre os dois níveis. Atende fato_analise_mensal, cujo granularidade é mensal e não poderia se ligar a um calendário diário sem explosão de cardinalidade.",
    "dim_cristalizador": "Cadastro dos 25 cristalizadores da salina. A área é constante por cristalizador na origem, portanto é atributo descritivo e não medida. Inclui classificação por tercil de área.",
    "dim_estacao": "Cadastro das 5 estações pluviométricas, com rótulos normalizados na camada Silver. Marca a estação com suspeita de descalibração a partir de 2024, permitindo gerar a análise com e sem ela.",
    "dim_fluido": "Fluidos medidos na bacia de evaporação. Os atributos físicos (diâmetro e área do espelho de água) não constam da origem: são conhecimento do processo declarado no pipeline, e permitem converter litros em lâmina milimétrica.",
    "fato_precipitacao_diaria": "Precipitação por estação e dia, série completa desde 2014. Registrada por evento e não diariamente: a ausência de linha não equivale a ausência de chuva, e nenhum preenchimento com zero é aplicado.",
    "fato_evaporacao_diaria": "Evaporação diária medida em bacia pelo método de reposição, por fluido. Medida consolidada com sinal: positivo é reposição (evaporação), negativo é retirada (excedente de chuva). Funciona como indicador climático composto, já que radiação, vento e nebulosidade não são medidos na salina.",
    "fato_laje_medicao": "Medição da laje por cristalizador, expressa em espessura da camada de sal (100 menos a leitura da régua), com as variações derivadas entre medições consecutivas. Marca eventos de colheita e cristalizadores recém-colhidos.",
    "fato_analise_mensal": "Tabela analítica na granularidade mês que cruza variação da laje, precipitação e evaporação. A chuva é acumulada na janela entre medições consecutivas, não no mês-calendário. Agregada por mês de propósito, para evitar pseudo-replicação: os 25 cristalizadores compartilham a mesma chuva e não são observações independentes.",
    "fato_chuva_mensal_historico": "Tabela analítica na granularidade mês do ano que compara a precipitação de 2026 com a média histórica de 2014 a 2025 do mesmo mês. Permite avaliar o deslocamento da estação chuvosa, que o total anual sozinho não revela. Exclui a estação com suspeita de descalibração.",
    "fato_chuva_anual": "Tabela analítica na granularidade ano que classifica cada ano como seco, típico ou chuvoso frente à média histórica. Usa o acumulado de janeiro a julho para que todos os anos sejam comparáveis, já que a série de 2026 termina em julho. O ano é dimensão degenerada: não há dim_ano porque todos os seus atributos seriam medidas.",
}

for tabela, comentario in comentarios_tabela.items():
    spark.sql(f"COMMENT ON TABLE {CATALOGO}.gold.{tabela} IS '{comentario}'")
print(f"Comentários aplicados a {len(comentarios_tabela)} tabelas.")

# COMMAND ----------

comentarios_coluna = {
    "fato_analise_mensal": {
        "ano_mes": "Competência da medição, yyyy-MM. Chave primária e estrangeira para dim_mes.",
        "cristalizadores": "Número de cristalizadores que entraram na média do mês, já excluídos os que passaram por colheita.",
        "variacao_media_cm": "Variação média da espessura da laje entre os cristalizadores. Positivo indica deposição de sal; negativo indica dissolução por chuva.",
        "variacao_desvio_cm": "Desvio-padrão da variação entre cristalizadores no mesmo mês. Mede a homogeneidade da resposta e justifica a agregação.",
        "dias_corridos_medio": "Intervalo médio entre medições consecutivas. Varia de 25 a 35 dias, o que exige normalização em qualquer comparação.",
        "medicoes_pos_colheita": "Medições cuja variação corresponde a um ciclo de formação iniciado do zero, logo após uma colheita.",
        "chuva_volume_mm": "Precipitação acumulada na janela entre medições, média das estações. NULO quando a janela não tem cobertura pluviométrica.",
        "chuva_dias_registrados": "Dias com precipitação registrada maior que zero na janela. É limite inferior: dias de chuva fraca sem leitura não aparecem.",
        "chuva_intensidade_mm_dia": "Volume dividido por dias registrados. Discriminador mais confiável entre regimes, por ser razão entre grandezas que sofrem o mesmo viés de observação.",
        "chuva_max_dia_mm": "Maior precipitação diária da janela. Representa o regime de evento extremo.",
        "dias_lidos_janela": "Dias com qualquer leitura de pluviômetro, inclusive zero. Mede o esforço de observação e expõe o acoplamento entre volume e frequência.",
        "evaporacao_agua_mm_dia": "Lâmina de água doce evaporada por dia na janela. Indicador climático composto de radiação, vento e nebulosidade.",
        "evaporacao_salmoura_mm_dia": "Lâmina de salmoura evaporada por dia na janela, em condição de produção.",
        "dias_retirada_janela": "Dias em que houve remoção de excedente da bacia. Confirmação de chuva independente do pluviômetro.",
        "cobertura_pluviometrica_ok": "Falso quando a série pluviométrica não alcança a janela. Nesse caso as medidas de chuva são nulas, e não zero.",
        "cobertura_bacia_ok": "Verdadeiro quando a série de bacias cobre a janela. Cobre agosto, período em que o pluviômetro não tem leitura.",
    },
    "fato_laje_medicao": {
        "cristalizador": "Identificador do cristalizador. Chave estrangeira para dim_cristalizador.",
        "data_medicao": "Data da medição. Chave estrangeira para dim_tempo.",
        "data_medicao_anterior": "Data da medição precedente do mesmo cristalizador. Define a janela de atribuição de chuva e evaporação.",
        "dias_corridos": "Intervalo real entre as duas medições, de 25 a 35 dias.",
        "espessura_laje_cm": "Espessura da camada de sal, em cm. Calculada como 100 menos a média das réguas, já que a régua registra a altura livre acima da laje. Cresce conforme o sal se deposita. Domínio observado: 0 a 25.",
        "espessura_anterior_cm": "Espessura na medição precedente.",
        "leitura_regua_cm": "Média das 8 leituras de régua conforme registrada em campo, preservada para rastreabilidade com a origem.",
        "amplitude_reguas_cm": "Diferença entre a maior e a menor régua da medição. Indicador de uniformidade da deposição.",
        "variacao_cm": "Variação da espessura da laje em relação à medição anterior. Positivo indica sal ganho, negativo indica sal perdido. Medida principal da análise.",
        "variacao_cm_dia": "Variação normalizada pelo intervalo real entre medições.",
        "flag_colheita": "Verdadeiro quando a espessura cai mais de 5 cm, indicando remoção da camada de sal e não efeito climático.",
        "crescimento_laje_cm": "Variação da espessura fora de períodos de colheita. Equivale a variacao_cm com os eventos de colheita excluídos.",
        "flag_laje_zerada": "Verdadeiro quando a espessura é zero, ou seja, cristalizador recém-colhido. Zero é o piso físico da medida, não falha de leitura.",
        "flag_base_zerada": "Verdadeiro quando a medição anterior tinha espessura zero, de modo que esta variação corresponde a um ciclo de formação iniciado do zero.",
    },
    "fato_evaporacao_diaria": {
        "fluido": "Fluido medido. Chave estrangeira para dim_fluido.",
        "data": "Data da medição. Chave estrangeira para dim_tempo.",
        "tipo_evento": "REPOSICAO quando houve reposição até a marca, RETIRADA quando houve remoção de excedente, SEM_LEITURA quando não houve registro.",
        "volume_litros": "Medida com sinal em litros: positivo é reposição, negativo é retirada.",
        "lamina_mm": "Volume dividido pela área do espelho de água. Padroniza a unidade para comparação direta com a precipitação.",
        "densidade_be": "Concentração da salmoura em graus Baumé. Nulo para água doce.",
        "variacao_densidade_be": "Variação da densidade em relação ao dia anterior. Queda acentuada indica diluição por chuva.",
    },
    "fato_chuva_anual": {
        "ano": "Ano civil. Dimensão degenerada: não existe dim_ano porque todos os seus atributos seriam medidas.",
        "total_mm_jan_jul": "Precipitação acumulada de janeiro a julho, média de todas as estações. Janela comparável entre todos os anos, já que 2026 termina em julho.",
        "total_mm_jan_jul_sem_suspeita": "Mesmo acumulado, excluindo a estação com suspeita de descalibração. É a medida usada na classificação.",
        "total_mm_ano": "Acumulado do ano completo. Não comparável para 2026, cuja série termina em julho.",
        "dias_com_chuva": "Dias distintos com precipitação registrada no ano.",
        "maior_evento_mm": "Maior precipitação diária registrada no ano em qualquer estação.",
        "media_historica_mm": "Média do acumulado de janeiro a julho entre 2014 e 2025, excluindo o ano do estudo.",
        "desvio_historico_mm": "Desvio-padrão da série histórica de janeiro a julho.",
        "desvio_vs_media_pct": "Distância percentual do ano em relação à média histórica.",
        "percentil_na_serie": "Percentual de anos históricos com acumulado inferior ao deste ano.",
        "classificacao": "SECO ou CHUVOSO quando o ano se afasta mais de um desvio-padrão da média histórica; TIPICO caso contrário.",
        "ano_do_estudo": "Marca o ano analisado nas perguntas 1 e 2.",
    },
}

total = 0
for tabela, colunas in comentarios_coluna.items():
    for coluna, texto in colunas.items():
        spark.sql(f"COMMENT ON COLUMN {CATALOGO}.gold.{tabela}.{coluna} IS '{texto}'")
        total += 1
print(f"Comentários aplicados a {total} colunas.")