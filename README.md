
<!-- README.md is generated from README.Rmd. Please edit that file -->

# geocodebr: Geolocalização de Endereços Brasileiros <img align="right" src="man/figures/logo.svg" alt="" width="180">

[![CRAN
status](https://www.r-pkg.org/badges/version/geocodebr)](https://CRAN.R-project.org/package=geocodebr)
[![CRAN/METACRAN Total
downloads](https://cranlogs.r-pkg.org/badges/grand-total/geocodebr?color=blue)](https://CRAN.R-project.org/package=geocodebr)
[![check](https://github.com/ipeaGIT/geocodebr/workflows/check/badge.svg)](https://github.com/ipeaGIT/geocodebr/actions)
[![Codecov test
coverage](https://codecov.io/gh/ipeaGIT/geocodebr/branch/main/graph/badge.svg)](https://app.codecov.io/gh/ipeaGIT/geocodebr?branch=main)
[![Lifecycle:
experimental](https://lifecycle.r-lib.org/articles/figures/lifecycle-experimental.svg)](https://lifecycle.r-lib.org/articles/stages.html)

O **{geocodebr}** é um pacote computacional para geolicalização de
endereços Brasileiros. O pacote oferece uma maneira simples e eficiente
de geolocalizar dados sem limite de número de consultas. O pacote é
baseado em conjuntos de dados espaciais abertos de endereços
brasileiros, utilizando como fonte principal o Cadastro Nacional de
Endereços para Fins Estatísticos (CNEFE). O CNEFE é
[publicado](https://www.ibge.gov.br/estatisticas/sociais/populacao/38734-cadastro-nacional-de-enderecos-para-fins-estatisticos.html)
pelo Instituto Brasileiro de Geografia e Estatística (IBGE). Atualmente,
o pacote está disponível apenas em R.

## Instalação

A última versão estável pode ser baixada do CRAN com o comando a seguir:

``` r
# from CRAN
install.packages("geocodebr")
```

Caso prefira, a versão em desenvolvimento:

``` r
# install.packages("remotes")
remotes::install_github("ipeaGIT/geocodebr")
```

## Utilização

O {geocodebr} possui três funções principais para geolocalização de
dados:

1.  `geocode()`
2.  `geocode_reverso()`
3.  `busca_por_cep()`

### 1. Geolocalização: de endereços para coordenadas espaciais

Uma que você possui uma tabela de dados (`data.frame`) com endereços no
Brasil, a geolocalização desses dados pode ser feita em apenas dois
passos:

1.  O primeiro passo é usar a função `definir_campos()` para indicar os
    nomes das colunas no seu `data.frame` que correspondem a cada campo
    dos endereços.

2.  O segundo passo é usar a função `geocode()` para encontrar as
    coordenadas geográficas dos endereços de input.

``` r
library(geocodebr)
library(sf)
#> Linking to GEOS 3.13.0, GDAL 3.10.1, PROJ 9.5.1; sf_use_s2() is TRUE

# carregando uma amostra de dados
input_df <- read.csv(system.file("extdata/small_sample.csv", package = "geocodebr"))

# Primeiro passo: inidicar o nome das colunas com cada campo dos enderecos
campos <- geocodebr::definir_campos(
  logradouro = "nm_logradouro",
  numero = "Numero",
  cep = "Cep",
  localidade = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf"
  )

# Segundo passo: geolocalizar
df <- geocodebr::geocode(
  enderecos = input_df,
  campos_endereco = campos,
  resultado_completo = FALSE,
  resolver_empates = FALSE,
  resultado_sf = FALSE,
  verboso = FALSE,
  cache = TRUE,
  n_cores = 1
  )
#> Warning: Foram encontrados 1 casos de empate. Estes casos foram marcados com valor
#> `TRUE` na coluna 'empate', e podem ser inspecionados na coluna
#> 'endereco_encontrado'. Alternativamente, use `resolver_empates = TRUE` para que
#> o pacote lide com os empates automaticamente. Ver documentação da função.
```

Os resultados do **{geocodebr}** são classificados em seis categorias gerais de 
`precisao`, dependendo do nível de exatidão com que cada endereço de input foi 
encontrado nos dados do CNEFE. Os resultados trazem ainda uma estimativa da 
incerteza da localização encontrado como um `desvio_metros`. Para mais 
informações, consulte a documentação da função ou a [**vignette
“geocode”**](https://ipeagit.github.io/geocodebr/articles/geocode.html).

### 2. Geolocalização reversa: de coordenadas espaciais para endereços

A função `geocode_reverso()`, por sua vez, permite a geolocalização
reversa, ou seja, a busca de endereços próximos a um conjunto de
coordenadas geográficas. A função pode ser útil, por exemplo, para
identificar endereços próximos a pontos de interesse, como escolas,
hospitais, ou locais de acidentes.

Mais detalhes na [**vignette
“geocode”**](https://ipeagit.github.io/geocodebr/articles/geocode_reverso.html).

``` r
# amostra de pontos espaciais
pontos <- readRDS(
   system.file("extdata/pontos.rds", package = "geocodebr")
   )

pontos <- pontos[1:20,]

# geocode reverso
df_enderecos <- geocodebr::geocode_reverso(
 pontos = pontos,
 dist_max = 1000,
 verboso = FALSE,
 n_cores = 1
 )
```

### 3. Busca por CEPs

Por fim, a função `busca_por_cep()` permite fazer consultas de CEPs para
encontrar endereços associados a cada CEP e suas coordenadas espaciais.

``` r
# amostra de CEPs
ceps <- c("70390-025", "20071-001")

df_ceps <- geocodebr::busca_por_cep(
 cep = ceps,
 resultado_sf = FALSE,
 verboso = FALSE
 )
```

## Nota <a href="https://www.ipea.gov.br"><img src="man/figures/ipea_logo.png" alt="IPEA" align="right" width="300"/></a>

Os dados originais do CNEFE são coletados pelo Instituto Brasileiro de
Geografia e Estatística (IBGE). O **{geocodebr}** foi desenvolvido por
uma equipe do Instituto de Pesquisa Econômica Aplicada (Ipea)

## Instituições utilizando o {geocodebr}

Além de diversos pesquisadores e empresas que utilizam o {geocodebr}, o
pacote também tem sido utilizado oficialmente por algumas instituições
públicas no planejamento e avaliação de políticas públicas. Entre elas:

- Banco Central do Brasil
- Ministério do Desenvolvimento Social e Combate à Fome (MDS)

## Projetos relacionados

Existem diversos pacotes de geolocalização disponíveis, muitos dos quais
podem ser utilizados em R (listados abaixo). A maioria dessas
alternativas depende de softwares e conjuntos de dados comerciais,
geralmente impondo limites de número de consultas gratuitas. Em
contraste, as principais vantagens do **{geocodebr}** são que o pacote:
(a) é completamente gratuito, permitindo consultas ilimitadas sem nenhum
custo; (b) opera com alta velocidade e escalabilidade eficiente,
permitindo geocodificar milhões de endereços em apenas alguns minutos,
sem a necessidade de infraestrutura computacional avançada ou de alto
desempenho.

- [{arcgisgeocode}](https://cran.r-project.org/package=arcgisgeocode)
  and [{arcgeocoder}](https://cran.r-project.org/package=arcgeocoder):
  utiliza serviço de geocode do ArcGIS
- [{nominatimlite}](https://cran.r-project.org/package=nominatimlite):
  baseado dados do OSM
- [{photon}](https://cran.r-project.org/package=photon): baseado dados
  do OSM
- [{tidygeocoder}](https://cran.r-project.org/package=tidygeocoder): API
  para diversos servicos de geolocalização
- [{googleway}](https://cran.r-project.org/package=googleway) and
  [{mapsapi}](https://cran.r-project.org/package=mapsapi): interface
  para API do Google Maps
