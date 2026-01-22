devtools::load_all('.')
library(ipeadatalake)
library(dplyr)
library(data.table)
library(enderecobr)
# library(mapview)
# library(sfheaders)
# library(sf)
# options(scipen = 999)
# mapview::mapviewOptions(platform = 'leafgl')
set.seed(42)

#' take-away
#' 1) a performance do geocodebr fica muito proxima do arcgis
#' 2) o que precisa fazer eh checar os casos em q a gente encontra com baixa
#' precisao e arcgis com alta. O que a gente pode fazer para melhorar o match?
#' Usar o LIKE logradouro na join ja melhorou muito, mas ainda daria pra melhorar?
#'
#' t <- subset(rais_like, match_type=='case_09' & Addr_type==	'PointAddress')

2+2
# stop()




# cad unico --------------------------------------------------------------------
sample_size <- 10000000

cad_con <- ipeadatalake::ler_cadunico(
  data = 202312,
  base = 'familia',
  as_data_frame = F,
  colunas = c("co_familiar_fam", "co_uf", "cd_ibge_cadastro",
              "no_localidade_fam", "no_tip_logradouro_fam",
              "no_tit_logradouro_fam", "no_logradouro_fam",
              "nu_logradouro_fam", "ds_complemento_fam",
              "ds_complemento_adic_fam",
              "nu_cep_logradouro_fam", "co_unidade_territorial_fam",
              "no_unidade_territorial_fam", "co_local_domic_fam")
  )

# a <- tail(cad, n = 100) |> collect()

# compose address fields
df <- cad_con |>
  mutate(no_tip_logradouro_fam = ifelse(is.na(no_tip_logradouro_fam), '', no_tip_logradouro_fam),
         no_tit_logradouro_fam = ifelse(is.na(no_tit_logradouro_fam), '', no_tit_logradouro_fam),
         no_logradouro_fam = ifelse(is.na(no_logradouro_fam), '', no_logradouro_fam)
         ) |>
  mutate(abbrev_state = co_uf,
          code_muni = cd_ibge_cadastro,
          logradouro = paste(no_tip_logradouro_fam, no_tit_logradouro_fam, no_logradouro_fam),
          numero = nu_logradouro_fam,
          cep = nu_cep_logradouro_fam,
          bairro = no_localidade_fam) |>
  select(co_familiar_fam,
         abbrev_state,
         code_muni,
         logradouro,
         numero,
         cep,
         bairro) |>
  dplyr::compute() |>
  dplyr::slice_sample(n = sample_size) |> # sample 20K
  dplyr::collect()

df$id <- 1:nrow(df)

campos <- geocodebr::definir_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  localidade = 'bairro',
  municipio = 'code_muni',
  estado = 'abbrev_state'
)

stop()


gc(T,T,T)
#bench::system_time(
 bench::mark(iterations = 1,
  cadgeo <- geocode(
    enderecos  = df,
    campos_endereco = campos,
    n_cores = 7, # 7
    verboso = T,
    resultado_completo = F,
    resolver_empates = T
    #resultado_sf = F
    #, h3_res = 9
    )
)


# 10 milhoes
# args: n_cores = 7, resultado_completo = F resolver_empates = T
# expression        min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
# v0.3.0 CRAN     29.7m  29.7m  0.000562    18.3GB   0.0725     1   129      29.7m <NULL> <Rprofmem>
# v0.4.0 CRAN     33.5m  33.5m  0.000497    8.06GB  0.00746     1    15      33.5m <NULL> <Rprofmem>
# v0.5.0 CRAN     6.04m  6.04m   0.00276     916MB  0.00276     1     1      6.04m <df>   <Rprofmem> <bench_tm> <tibble>
# v0.6.0 dev      4.64m  4.64m   0.00359     916MB  0.00359     1     1      4.64m <df>

 1 cadgeo <- geocode(e… 5.17m  5.17m   0.00323    1014MB  0.00645     1     2      5.17m <df>

 # v0.5.0 CRAN     2.39m em paralelo


# args: n_cores = 7, resultado_completo = F resolver_empates = T
# v0.5.0 CRAN     6.04m  6.04m   0.00276     916MB  0.00276     1     1      6.04m <df>   <Rprofmem> <bench_tm> <tibble>
# main            6.52m  6.52m   0.00256    1014MB  0.00767     1     3      6.52m <df>   <Rprofmem>



# 43 milhoes
# args: n_cores = 7, resultado_completo = F resolver_empates = T
# expression        min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
# v0.3.0 CRAN        2h     2h  0.000139    79.3GB   0.0176     1   127         2h <dt>
# v0.4.0 CRAN      3.3h   3.3h 0.0000843    34.5GB  0.00244     1    29       3.3h <dt>   <Rprofmem> <bench_tm> <tibble>
# v0.5.0 dev
# v0.5.0 devcallr  21.1m  21.1m  0.000791    4.12GB 0.000791     1     1      21.1m <dt>   <Rprofmem>
# v0.5.0 devcallr  8.99m em paralelo por uf

# v0.5.0 + Rust    16.9m  16.9m  0.000985    4.11GB 0.000985     1     1      16.9m <df>



# nao era para ser empate
# 5 "da02" "da02"   mesmo rua e cep
# [1] "RUA PAULO SIMOES DA COSTA, 32 (aprox) - JARDIM ANGELA, SAO PAULO - SP, 04929-140"
# [2] "RUA PAULO SIMOES DA COSTA, 32 (aprox) - ALTO DO RIVIERA, SAO PAULO - SP, 04929-140"

 # 10 milhoes com callr
#                             step_sec total_sec step_relative
#                       Start     0.06      0.06           0.0
#                Padronizacao   139.62    139.68          49.2
# Register standardized input    22.08    161.76           7.8
#                    Matching    87.11    248.87          30.7
#             Resolve empates     6.55    255.42           2.3
#   Write original input back     4.19    259.61           1.5
#               Add precision     0.34    259.95           0.1
#               Merge results    23.80    283.75           8.4




## cadunico parallel callr ----------------

library(future.callr)
library(future)
library(furrr)

future::plan(future.callr::callr)
future::plan(future.callr::callr)

df$abbrev_state <- enderecobr::padronizar_estados(df$abbrev_state)

gc(T,T,T)
bench::bench_time(
 a <-   split(df, f = df$abbrev_state) |>
   furrr::future_map(
     .progress = TRUE,
     .f = function(x){
       geocode(
         enderecos = x,
         campos_endereco = campos,
         resultado_completo = F,
         resolver_empates = T
       )
     }
   )

)

bench::bench_time(

a <- data.table::rbindlist(a)
)

quantile(a$desvio_metros, na.rm = T, probs = c(0.5, 0.7, 0.75, 0.8, 0.85, 0.9))
  # 50%   70%   75%   80%   85%   90%
  #   7   228   451   864  2418 10763


# rais --------------------------------------------------------------------

rais <- ipeadatalake::ler_rais(
  ano = 2019,
  tipo = 'estabelecimento',
  as_data_frame = F,
  geoloc = T) |>
  select("id_estab", "logradouro", "bairro", "codemun", "uf", "cep",
         'lat', 'lon', 'Addr_type', 'Match_addr') |>
  compute() |>
  dplyr::slice_sample(n = 1000000) |> # sample 10 million
  filter(uf != "IG") |>
  filter(uf != "") |>
  collect()


# rais <- head(rais, n = 1000) |> collect() |> dput()
data.table::setDT(rais)

# create column number
rais[, numero := gsub("[^0-9]", "", logradouro)]

# remove numbers from logradouro
rais[, logradouro_no_numbers := gsub("//d+", "", logradouro)]
rais[, logradouro_no_numbers := gsub(",", "", logradouro_no_numbers)]

rais[, id := 1:nrow(rais)]

data.table::setnames(
  rais,
  old = c('lat', 'lon'),
  new = c('lat_arcgis', 'lon_arcgis')
)


head(rais)


fields <- geocodebr::definir_campos(
  logradouro = 'logradouro_no_numbers',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'codemun',
  estado = 'uf'
)



rafa <- function(){ message('rafa')
  rais_geo <- geocodebr::geocode(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 7,
    full_results =  T,
    progress = T
  )
}

table(rais_geo$precision) / nrow(rais_geo) *100


mb <- microbenchmark::microbenchmark(
  rafa = rafa(),
  times  = 2
)

mb
# 8.6 milhoes de linhas
# Unit: seconds
#       expr      min       lq     mean   median       uq      max neval
#       dani 423.3079 423.3079 423.3079 423.3079 423.3079 423.3079     1
#       rafa 542.9040 542.9040 542.9040 542.9040 542.9040 542.9040     1
# rafa_arrow 260.3829 260.3829 260.3829 260.3829 260.3829 260.3829     1

# com matched address e todas categorias
# Unit: seconds
# expr      min      lq    mean   median       uq      max neval
# rafa 468.2382 835.071 1275.96 1286.295 1699.844 2090.351     5



rafaF <- function(){ message('rafa F')
  rais <- geocodebr::geocode(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    full_results = F,
    progress = T
  )
  return(2+2)
}



rafaF_db <- function(){ message('rafa F')
  df_rafaF <- geocodebr:::geocode_db(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    full_results = F,
    progress = T
  )
  return(2+2)
}

rafaT_db <- function(){ message('rafa T')
  df_rafaT <- geocodebr:::geocode_db(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    full_results = T,
    progress = T
  )
  return(2+2)
}

rafaT <- function(){ message('rafa T')
  df_rafaT <- geocodebr::geocode(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    full_results = T,
    progress = T
  )
  return(2+2)
}

mb <- microbenchmark::microbenchmark(
  rafa_drop = rafaF(),
  rafa_keep = rafaT(),
  rafa_drop_db = rafaF_db(),
  rafa_keep_db = rafaT_db(),
  times  = 5
)
mb


bm <- bench::mark(
  rafa_drop = rafaF(),
  rafa_keep = rafaT(),
  rafa_drop_db = rafaF_db(),
  rafa_keep_db = rafaT_db(),
  check = F,
  iterations  = 1
)
bm


# Unit: seconds
#    expr       min        lq     mean    median       uq      max neval
#    rafa_drop  320.9953  460.6672 1274.692  685.1642 2378.397 2528.236     5
#    rafa_keep 1397.4498 2468.7379 2887.765 3072.3166 3670.223 3830.096     5
# rafa_drop_db 2387.5650 2449.5906 2527.181 2569.6456 2584.436 2644.668     5
# rafa_keep_db 2060.3775 2823.0493 3194.852 3383.1116 3485.412 4222.308     5







data.table::setnames(rais, old = 'match_type', new = 'match_type_equal')
data.table::setnames(rais, old = 'lon', new = 'lon_equal')
data.table::setnames(rais, old = 'lat', new = 'lat_equal')

rais_like <- geocodebr:::geocode_like(
  addresses_table = rais,
  address_fields = fields,
  n_cores = 20, # 7
  progress = F
)

tictoc::toc()

table(rais_like$match_type_equal, rais_like$match_type)

result_arcgis <- table(rais_like$Addr_type) / nrow(rais_like) *100
result_geocodebr <- table(rais_like$match_type) / nrow(rais_like) *100

aaaa <- table(rais_like$match_type, rais_like$Addr_type) / nrow(rais_like) *100
aaaa <- as.data.frame(aaaa)
aaaa <- subset(aaaa, Freq>0)


data.table::fwrite(aaaa, 'rais.csv', dec = ',', sep = '-')



t <- subset(rais_like, match_type=='case_09' & Addr_type==	'PointAddress')

t_arc <- sfheaders::sf_point(t[1,], x = 'lon_arcgis', y = 'lat_arcgis',keep = T)
t_geo <- sfheaders::sf_point(t[1,], x = 'lon', y = 'lat',keep = T)

st_crs(t_arc) <- 4674
st_crs(t_geo) <- 4674

mapview::mapviewOptions(platform = 'mapdeck', )

mapview(t_arc) + t_geo
sf::st_distance(t_geo, t_arc)



jp <- geocodebr::get_cache_dir() |>
  geocodebr:::arrow_open_dataset()  |>
  filter(estado=="PB") |>
  filter(municipio == "JOAO PESSOA") |>
  collect()

head(jp)

subset(jp , logradouro_sem_numero %like% "DESEMBARGADOR SOUTO MAIOR")
subset(t , logradouro_no_numbers %like% "DESEMBARGADOR SOUTO MAIOR")






# censo escolar ---------------------------------

censo_escolar <- ipeadatalake::ler_censo_escolar(
  ano = 2022,
  base = 'basica'
  )  |>
  select(
    c("NU_ANO_CENSO",                "NO_REGIAO",
      "CO_REGIAO",                   "NO_UF",
      "SG_UF",                       "CO_UF",
      "NO_MUNICIPIO",                "CO_MUNICIPIO",
      "NO_MESORREGIAO",              "CO_MESORREGIAO",
      "NO_MICRORREGIAO",             "CO_MICRORREGIAO",
      "CO_DISTRITO",                 "NO_ENTIDADE",
      "CO_ENTIDADE",                 "TP_DEPENDENCIA",
      "TP_CATEGORIA_ESCOLA_PRIVADA", "TP_LOCALIZACAO",
      "TP_LOCALIZACAO_DIFERENCIADA", "DS_ENDERECO",
      "NU_ENDERECO",                 "DS_COMPLEMENTO",
      "NO_BAIRRO",                   "CO_CEP",
      "NU_DDD")
    ) |>
  dplyr::collect()


censo_escolar$id <- 1:nrow(censo_escolar)

fields_cad <- geocodebr::definir_campos(
  logradouro = 'DS_ENDERECO',
  numero = 'NU_ENDERECO',
  cep = 'CO_CEP',
  localidade = 'NO_BAIRRO',
  municipio = 'NO_MUNICIPIO',
  estado = 'NO_UF'
)

# bench::mark( iterations = 1,
bench::system_time(
  geo <- geocodebr::geocode(
    enderecos  = censo_escolar,
    campos_endereco = fields_cad,
    resultado_completo = T,
    n_cores = 25, # 7
    verboso = T,
    resultado_sf = F,
    resolver_empates = F
  )
)



