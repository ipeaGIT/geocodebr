
<!-- README.md is generated from README.Rmd. Please edit that file -->

# geocodebr

[![CRAN
status](https://www.r-pkg.org/badges/version/geocodebr)](https://CRAN.R-project.org/package=geocodebr)
[![check](https://github.com/ipeaGIT/geocodebr/workflows/check/badge.svg)](https://github.com/ipeaGIT/geocodebr/actions)
[![Codecov test
coverage](https://codecov.io/gh/ipeaGIT/geocodebr/branch/main/graph/badge.svg)](https://app.codecov.io/gh/ipeaGIT/geocodebr?branch=main)
[![Lifecycle:
experimental](https://lifecycle.r-lib.org/articles/figures/lifecycle-experimental.svg)](https://lifecycle.r-lib.org/articles/stages.html)

**{geocodebr}** is a package to geocode data in Brazil. It provides a
simple and efficient way to geocode from addresses to spatial
coordinates and to reverse-geocode from spatial coordinates to
addresses. The package builds on open spatial data sets of Brazilian
addresses, mainly using the National Registry of Addresses for
Statistical Purposes (English for *Cadastro Nacional de Endereços para
Fins Estatísticos*, CNEFE). CNEFE is
[published](https://www.ibge.gov.br/estatisticas/sociais/populacao/38734-cadastro-nacional-de-enderecos-para-fins-estatisticos.html)
by the Brazilian official statistics and Geography Office (IBGE) The
package is currently available in R.

## Installation

The package is not yet on CRAN. You can install the development version
with:

``` r
# install.packages("pak")
pak::pak("ipeaGIT/geocodebr")
```

## Basic Usage

### Geocoding: from addresses to spatial coordinates

Once you have a table (`data.frame`) with addresses, geolocating the
data with **{geocodebr}** can be done in two simple steps:

1.  The first step is to use the `listar_campos()` function to declare
    the names of the columns in your input `data.frame` that correspond
    to each field of the addresses.

2.  The second step is to use the `geocode()` function to find the
    geographic coordinates of input addresses.

``` r
library(geocodebr)

# carregando uma amostra de dados
input_df <- read.csv(system.file("extdata/small_sample.csv", package = "geocodebr"))

# Primeiro passo: inidicar o nome das colunas com cada campo dos enderecos
campos <- geocodebr::listar_campos(
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
  verboso = TRUE,
  cache = TRUE,
  n_cores = 1
  )
```

The results of {geocodebr} are classified into six broad `precision`
categories depending on how exactly each input address was matched with
CNEFE data. See more information in the function documentation or in the
**geocoding vignette**.

### Reverse-geocoding: from spatial coordinates to addresses

*soon*

## Related projects

There are numerous geolocation packages available, many of which can be
used in R (list below). Most of these alternatives rely on commercial
software and data sets, often imposing strict limits on the number of
free queries. In contrast, the main advantages of {geocodebr} are that
(a) it is completely free, allowing unlimited queries without any cost;
(b) it operates with high speed and efficient scalability, allowing one
to geocode millions of addresses in just a few minutes without requiring
advanced or high-end computational infrastructure.

- [{arcgisgeocode}](https://cran.r-project.org/web/packages/arcgisgeocode/index.html)
  and
  [{arcgeocoder}](https://cran.r-project.org/web/packages/arcgeocoder/index.html):
  interface to ArcGIS Geocoding Services
- [{nominatimlite}](https://cran.r-project.org/web/packages/nominatimlite/index.html):
  geocode based on OSM data
- [{photon}](https://cran.r-project.org/web/packages/photon/index.html):
  based on OSM data
- [{tidygeocoder}](https://cran.r-project.org/web/packages/tidygeocoder/index.html):
  interface several geocoding services
- [{googleway}](https://cran.r-project.org/web/packages/googleway/index.html)
  and
  [{mapsapi}](https://cran.r-project.org/web/packages/mapsapi/index.html):
  interface to Google Maps APIs

## Acknowledgement <a href="https://www.ipea.gov.br"><img src="man/figures/ipea_logo.png" alt="IPEA" align="right" width="300"/></a>

Original CNEFE data is collected by the Brazilian Institute of Geography
and Statistics (IBGE). **{geocodebr}** is developed by a team at the
Institute for Applied Economic Research (Ipea), Brazil.
