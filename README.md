# mvp-dataengineering — Impacto das chuvas na produção de sal marinho

Repositório da matéria de Engenharia de Dados do curso de Pós-Graduação em Ciência de Dados e Analytics da PUC-Rio. Contém o pipeline de engenharia de dados em Databricks que analisa a relação entre precipitação e a espessura da laje de sal em cristalizadores.

### Autor: Vinicius Leonardo Lança

## Documentação

O trabalho completo está em **[`MVP-EngenhariaDados-PUC.pdf`](MVP-EngenhariaDados-PUC.pdf)**: contexto de negócio, perguntas de pesquisa, modelagem, catálogo de dados, análise de qualidade, resultados e autoavaliação. É o documento a ler primeiro — os notebooks deste repositório são a implementação do que está descrito nele.

## Ordem de execução

### Pipeline

| # | Notebook | O que faz |
|---|---|---|
| 1 | `MVP01-Setup` | Cria catálogo e schemas |
| 2 | `MVP02-Download` | Coleta no Google Drive e carga na Staging |
| 3 | `MVP03-Bronze` | Staging → Bronze |
| 4 | `MVP04-Silver` | Bronze → Silver, com tratamento de qualidade |
| 5 | `MVP05-Gold` | Silver → Gold, modelo dimensional |

### Notebooks complementares

| Notebook | Quando executar |
|---|---|
| `01.Catalogo_de_dados` | Depois do `MVP05-Gold`, para registrar as descrições no catálogo |
| `02.Perfil_de_Dados` | Depois do `MVP03-Bronze`, para gerar o perfil estatístico das tabelas |

Nenhum dos dois altera dados. Podem ser reexecutados quantas vezes for preciso.

## Catálogo de dados

O notebook `01.Catalogo_de_dados` registra, via `COMMENT ON TABLE` e `COMMENT ON COLUMN`, a descrição de cada tabela e de cada coluna das camadas Silver e Gold. As descrições ficam visíveis no Catalog Explorer, no autocomplete do editor SQL e nas APIs de metadados — o catálogo é a própria plataforma, e o arquivo em `docs/` é apenas uma cópia para leitura fora dela.

O notebook é idempotente: pode ser reexecutado a qualquer momento sem efeito colateral. A última célula lista as colunas que ainda estejam sem descrição, e deve retornar vazia.

    SELECT table_schema, table_name, column_name
    FROM mvpdataengineer.information_schema.columns
    WHERE table_schema IN ('silver', 'gold')
      AND (comment IS NULL OR trim(comment) = '');

## Perfil de dados

O notebook `02.Perfil_de_Dados` gera as estatísticas descritivas das tabelas da camada Bronze: valores ausentes, distintos, extremos, distribuição das colunas numéricas e valores mais frequentes das colunas categóricas. É a varredura inicial que sustenta a seção de qualidade de dados do trabalho.

O perfil foi escrito com API padrão do Spark em vez de `dbutils.data.summarize()`, que não é suportado em compute serverless. A implementação própria também trata o formato dos dados da Bronze, que guarda tudo como texto: converte vírgula decimal antes de calcular e conta hífen e célula vazia como ausência, em vez de lê-los como texto válido.

A última célula exporta o resultado em markdown, para acompanhar o trabalho como evidência reexecutável.

## Dados

As bases de origem não acompanham o repositório. São dados operacionais privados, descaracterizados, cujas condições de uso estão descritas na documentação do trabalho.
