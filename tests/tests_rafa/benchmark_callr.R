df <- readRDS('lento.rds')

head(df)



# benchmark different approaches ------------------------------------------------------------------


campos <- geocodebr::definir_campos(
  logradouro = 'logradouro_padr_orig',
  numero = 'numero_padr_orig',
  cep = 'cep_padr_orig',
  localidade = 'bairro_padr_orig',
  municipio = 'municipio_padr_orig',
  estado = 'estado_padr_orig'
)


gc(full = T)
bench::bench_time(

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
#              | process  real
#  v0.4.0 CRAN | 49.89s  8.76m
#  register    | 48.95s  7.99m
#  temp view   | 49.03s  8.75m
#  temp table  | 47.84s  7.18m

# 10 milhoes cad unico direto
#             | process  real
# v0.4.0 CRAN | 33.4m   18.1m
# register    |   30m   14.9m
# temp view   |  1.1h   39.7m
# temp table  | 22.1m   9.42m


# 43 milhoes cad unico direto
#             |
# v0.4.0 CRAN | 2.98m  25.93m callr
# register    | 3.4m   28.8m  callr
# temp table  | 3.14m  26.62m callr

# v0.4.0 CRAN | 2.05h   1.38h direto
# register    |               direto  !!!!!!!!!!!!!!!!!!!
# temp table  | 6.19h   5.05h direto !!!!!!!!!!!!!!!!!!!





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
