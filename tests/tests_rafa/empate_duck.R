devtools::load_all('.')
library(dplyr)
library(enderecobr)
library(data.table)
library(arrow)
library(duckdb)



# open input data
data_path <- system.file("extdata/large_sample.parquet", package = "geocodebr")
input_df <- arrow::read_parquet(data_path)


ncores <- 7


campos <- geocodebr::definir_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  localidade = 'bairro',
  municipio = 'municipio',
  estado = 'uf'
)

# enderecos = input_df
# n_cores = 7
# ncores <- 7
# verboso = T
# cache = TRUE
# resultado_completo = T
# resultado_sf = F
# campos_endereco <- geocodebr::definir_campos(
#   logradouro = 'logradouro',
#   numero = 'numero',
#   cep = 'cep',
#   localidade = 'bairro',
#   municipio = 'municipio',
#   estado = 'uf')
# resolver_empates = T

bench::mark( iterations = 1,
             geo_dt <- geocodebr::geocode(
               enderecos = input_df,
               campos_endereco = campos,
               n_cores = ncores,
               resultado_completo = T,
               verboso = T,
               resultado_sf = F,
               resolver_empates = F
             )
)

# expression             min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
#    dt 729            28.4s  28.4s    0.0352    64.1MB    0.281     1     8      28.4s <dt>
# duck2 700            28.9s  28.9s    0.0346    64.7MB    0.277     1     8      28.9s <df>

bench::mark( iterations = 1,
             geo_duck <- geocodebr::geocode_duckdb(
               enderecos = input_df,
               campos_endereco = campos,
               n_cores = ncores,
               resultado_completo = T,
               verboso = T,
               resultado_sf = F,
               resolver_empates = F
             )
)

identical(geo_dt, geo_duck)

# expression             min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
#  geo_dt F            34.9s  34.9s    0.0287    64.2MB    0.258     1     9      34.9s <dt>
#  geo_duck F            31s    31s    0.0322    40.8MB    0.258     1     8        31s <dt>
#
#  geo_dt T            30.9s  30.9s    0.0323      52MB    0.291     1     9      30.9s <dt>
#  geo_duck T            29.7s  29.7s    0.0337    40.4MB    0.269     1     8      29.7s <dt>


identical(geo_dt, geo_duck)
identical(geo_dt$empate, geo_duck$empate)



a=geo_dt[id==17696]
b=geo_duck[id==17696]
