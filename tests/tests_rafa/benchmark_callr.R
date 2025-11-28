df <- readRDS('lento.rds')

head(df)

campos <- geocodebr::definir_campos(
  logradouro = 'logradouro_padr_orig',
  numero = 'numero_padr_orig',
  cep = 'cep_padr_orig',
  localidade = 'bairro_padr_orig',
  municipio = 'municipio_padr_orig',
  estado = 'estado_padr_orig'
)




# benchmark different approaches ------------------------------------------------------------------

gc(full = T)
bench::bench_time(
#bench::mark(
  callr::r(function(df, campos) {
    rfiles <- list.files("R", full.names = TRUE)
    invisible(lapply(rfiles, source))
    library("data.table")
    # library("geocodebr")
    v3 <- geocode(
      enderecos = df,
      campos_endereco = campos,
      n_cores = 7,
      resultado_completo = F,
      verboso = T,
      resultado_sf = T,
      resolver_empates = T,
      h3_res = 9)
  },
  args = list(df = df, campos = campos),
  show = TRUE
  )
)


gc(full = T)
bench::bench_time(
    v3 <- geocode(
      enderecos = df,
      campos_endereco = campos,
      n_cores = 7,
      resultado_completo = F,
      verboso = T,
      resultado_sf = T,
      resolver_empates = T,
      h3_res = 9)
)


# 10 milhoes cad unico caller
#              | process   real
#  v0.4.0 CRAN | 49.89s   8.76m
#  register    | 49.25s   7.88m
#  temp table  | 46.66s   7.05m (antigo calc dist 7.08m)

#   expression    min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time       gc
#   temp table  7.04m  7.04m   0.00237     916MB  0.00946     1     4      7.04m <sf>   <Rprofmem> <bench_tm> <tibble>


# 10 milhoes cad unico direto
#             | process   real
# v0.4.0 CRAN | 33.4m    18.1m
# register    | 32.0m    16.1m
# temp table  | 22.02m    9.33 (antigo calc dist 10.3m )


# 43 milhoes cad unico callr
#             |
# v0.4.0 CRAN | 2.98m  25.93m
# register    |  2.9m  26.50m
# temp table  | 3.53m  26.20m

#   expression         min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
# 1 temp table callr 26.8m  26.8m  0.000622    4.11GB  0.00186     1     3      26.8m <sf>
# 1 temp table diret 3.63h  3.63h  0.000076    41.8GB  0.00336     1    44      3.63h <sf>
3.63h  wtf?

# 43 milhoes cad unico direto
#
# v0.4.0 CRAN | 2.05h   1.38h
# register    | 2.17h   1.28h
# temp table  | 1.75h   1.01h





# 33 mil escolas
#  process     real
#  515.6ms   1.36m register
#  437.5ms   33.2s write temp view
#  437.5ms   33.2s write temp table


# devtools::load_all('.')
#
# data_path <- system.file("extdata/large_sample.parquet", package = "geocodebr")
# input_df <- arrow::read_parquet(data_path)
#
# ncores <- 7
#
#
# campos <- geocodebr::definir_campos(
#   logradouro = 'logradouro',
#   numero = 'numero',
#   cep = 'cep',
#   localidade = 'bairro',
#   municipio = 'municipio',
#   estado = 'uf'
# )
#
# bench::bench_time(
#
#              callr::r(function(df, campos) {
#                rfiles <- list.files("R", full.names = TRUE)
#                invisible(lapply(rfiles, source))
#                library("data.table")
#                v3 <- geocode(
#                  enderecos = df,
#                  campos_endereco = campos,
#                  n_cores = 7,
#                  resultado_completo = F,
#                  verboso = T,
#                  resultado_sf = T,
#                  resolver_empates = T,
#                  h3_res = 9)
#              },
#              args = list(df = df, campos = campos),
#              show = TRUE
#              )
# )



#   expression                                    min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time       gc
#   <bch:expr>                                  <bch> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>     <list>     <list>
#  register arrow                               16.3m  16.3m   0.00103    64.4MB  0.00615     1     6      16.3m <sf>   <Rprofmem> <bench_tm> <tibble>
#  write table                                  40.2s  40.2s    0.0249    73.5MB    0.871     1    35      40.2s <sf>   <Rprofmem> <bench_tm> <tibble>

















# calc dist novo
#    step_sec total_sec step_relative
#                        Start     0.22      0.22           0.0
#                 Padronizacao   490.67    490.89          36.5
#  Register standardized input    88.40    579.29           6.6
#                     Matching   446.18   1025.47          33.2
#              Resolve empates    37.48   1062.95           2.8
#    Write original input back    17.36   1080.31           1.3
#                Add precision     1.75   1082.06           0.1
#                Merge results    79.36   1161.42           5.9
#                       Add H3    51.98   1213.40           3.9
#                Convert to sf   131.89   1345.29           9.8
# Warning message:
# In enderecobr::padronizar_enderecos(enderecos, campos_do_endereco = enderecobr::correspondencia_campos(logradouro = campos_endereco[["logradouro"]],  :
#   Alguns números não puderam ser convertidos para integer, introduzindo NAs no
# resultado.
# process    real
#    3.3m   27.9m



# calc dist antigo
#                        step— Timing summary —
#  step_sec total_sec step_relative
#                        Start     0.28      0.28           0.0
#                 Padronizacao   521.70    521.98          37.9
#  Register standardized input    88.34    610.32           6.4
#                     Matching   457.97   1068.29          33.3
#              Resolve empates    27.14   1095.43           2.0
#    Write original input back    16.99   1112.42           1.2
#                Add precision     1.67   1114.09           0.1
#                Merge results    76.12   1190.21           5.5
#                       Add H3    52.21   1242.42           3.8
#                Convert to sf   133.89   1376.31           9.7
# Warning message:
# In enderecobr::padronizar_enderecos(enderecos, campos_do_endereco = enderecobr::correspondencia_campos(logradouro = campos_endereco[["logradouro"]],  :
#   Alguns números não puderam ser convertidos para integer, introduzindo NAs no
# resultado.
# process    real
#   3.37m  28.53m
