# mvp-dataengineering - Impacto das chuvas na produção de sal marinho

Repositório da matéria de Engenharia de Dados do curso de Pós-Graduação em Ciência de Dados e Analytics da PUC-Rio. Contem o pipeline de engenharia de dados em Databricks que analisa a relação entre precipitação e a espessura da laje de sal em cristalizadores.

## Ordem de execução

1. `MVP01-Setup`     — cria catálogo e schemas
2. `MVP02-Download`  — coleta no Google Drive e carga na Staging
3. `MVP03-Bronze`    — Staging → Bronze
4. `MVP04-Silver`    — Bronze → Silver, com tratamento de qualidade
5. `MVP05-Gold`      — Silver → Gold, modelo dimensional

## Documentação



## Catálogo de dados

O notebook `Catalogo_de_Dados` registra, via `COMMENT ON TABLE` e `COMMENT ON COLUMN`, a descrição de cada tabela e de cada coluna das camadas Silver e Gold. As descrições ficam visíveis no Catalog Explorer, no autocomplete do editor SQL e nas APIs de metadados — o catálogo é a própria plataforma, e o arquivo em `docs/` é apenas uma cópia para leitura fora dela.

O notebook é idempotente: pode ser reexecutado a qualquer momento sem efeito colateral.
A última célula lista as colunas que ainda estejam sem descrição, e deve retornar vazia.

    SELECT table_schema, table_name, column_name
    FROM mvpdataengineer.information_schema.columns
    WHERE table_schema IN ('silver', 'gold')
      AND (comment IS NULL OR trim(comment) = '');


## Dados

As bases de origem não acompanham o repositório. São dados operacionais privados, descaracterizados, cujas condições de uso estão descritas na documentação do trabalho.
