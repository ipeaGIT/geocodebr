# Faz download dos dados do CNEFE

Faz o download de uma versão pre-processada e enriquecida do CNEFE
(Cadastro Nacional de Endereços para Fins Estatísticos) que foi criada
para o uso deste pacote.

## Usage

``` r
download_cnefe(tabela = "todas", verboso = TRUE, cache = TRUE)
```

## Arguments

- tabela:

  Nome da tabela para ser baixada. Por padrão, baixa `"todas"`.

- verboso:

  Um valor lógico. Indica se barras de progresso e mensagens devem ser
  exibidas durante o download dos dados do CNEFE e a geocodificação dos
  endereços. O padrão é `TRUE`.

- cache:

  Um valor lógico. Indica se os dados do CNEFE devem ser salvos ou lidos
  do cache, reduzindo o tempo de processamento em chamadas futuras. O
  padrão é `TRUE`. Quando `FALSE`, os dados do CNEFE são baixados para
  um diretório temporário.

## Value

Retorna o caminho para o diretório onde os dados foram salvos.

## Examples

``` r
download_cnefe(verboso = FALSE)
```
