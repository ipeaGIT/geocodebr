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
sample_size <- 1000000

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
cad <- cad_con |>
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
  # dplyr::slice_sample(n = sample_size) |> # sample 20K
  dplyr::collect()


# setDT(cad)
#
# cad[, logradouro := enderecobr::padronizar_logradouros(logradouro) ]
# cad[, numero := enderecobr::padronizar_numeros(numero,formato = 'integer') ]
# cad[, cep := enderecobr::padronizar_ceps(cep) ]
# cad[, bairro := enderecobr::padronizar_bairros(bairro) ]
# cad[, code_muni := enderecobr::padronizar_municipios(code_muni) ]
# cad[, abbrev_state := enderecobr::padronizar_estados(abbrev_state, formato = 'sigla') ]


cad$id <- 1:nrow(cad)

fields_cad <- geocodebr::definir_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  localidade = 'bairro',
  municipio = 'code_muni',
  estado = 'abbrev_state'
)



#' Cad completo 43 milhoes
#'
#' > rafa_drop
#' process    real
#' 23.9m     17m
#' expression           min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
#' determ_wth_matches 2.15h  2.15h  0.000129    73.7GB   0.0107     1    83      2.15h / 2,204,315 empates
#' prob after all det 2.64h  2.64h  0.000105    67.4GB   0.0110     1   104      2.64h / 2,444,248 empates
#' ideal seq          2.89h  2.89h 0.0000962    66.6GB   0.0123     1   128      2.89h / 2,296,752 empates
#' 2nd best seq       2.98h  2.98h 0.0000934    67.1GB   0.0114     1   122      2.98h / 2,409,066 empates



# sequencia de matches
# 2nd best  1.18h    2296759
# ideal seq 1.21h    2296827  empates
# determ    46.06m   2204536

# ℹ Geolocalizando endereços
# Error in `duckdb_result()`:70,985/43,882,017 ■■■■■■■■■■■■■■■■■■■■■■            69% - Procurando pl02
# ! rapi_execute: Failed to run query
# Error: Out of Memory Error: Allocation failure
# Run `rlang::last_trace()` to see where the error occurred.
#



# bench::mark( iterations = 1,
 bench::mark(
  cadgeo <- geocodebr::geocode(
    enderecos  = cad,
    campos_endereco = fields_cad,
    resultado_completo = T,
    n_cores = 12, # 7
    verboso = T,
    resultado_sf = F,
    resolver_empates = T
    )
)

# 1 milhao
# seque best 2.15m // 69723 empates
# 2nd   best 2.1m // 72743 empates

# 25 milhoes
# seque best 49.4m // 1268668 empates
# 2nd   best 42.9m // 1331655 empates


# novo prob 4.44m // 66561 empates
# antg prob 15.8m // 77189 empates

# 1 milhao
# expression   min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     EMPATES
#        old 4.66m  4.66m   0.00358    1.82GB    0.200     1    56      4.66m <dt>   <Rprofmem> 70940 vs 70940
#        new 7.33m  7.33m   0.00227    1.83GB    0.173     1    76      7.33m <dt>   <Rprofmem> 71402  vs 71390
#   20250318 5.26m  5.26m   0.00317     1.8GB    0.181     1    57      5.26m <dt>   <Rprofmem> <bench_tm>
#> com arrow unique

#   20250319 5.53m  5.53m   0.00301    1.82GB    0.154     1    51      5.53m <dt>   <Rprofmem> 67285
#> arrow unique + sendQuerryarrow

#   20250320 5.98m  5.98m   0.00279    1.82GB    0.173     1    62      5.98m <dt>   <Rprofmem> 67409
#> arrow unique + sendQuerryarrow e NO NUMBER nas cat 'pn'


# 20250318 1.28h  1.28h  0.000217    1.83GB   0.0141     1    65      1.28h <dt>   <Rprofmem> <bench_tm> <tibble> 67608
#> com duckdb unique

# 3.61m  3.61m   0.00462    1.83GB    0.365     1    79      3.61m <dt>   <Rprofmem> // 69725
# latest



# 500K
# expression   min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     EMPATES
#       old 9.56m  9.56m   0.00174    1.02GB   0.0994     1    57      9.56m <dt>   <Rprofmem>  47,258
#       new 1.99m  1.99m   0.00839    1000MB    0.327     1    39      1.99m <dt>   <Rprofmem>  41,317
#  20250318 2.26m  2.26m   0.00736    1003MB    0.434     1    59      2.26m <dt>   <Rprofmem>

#  20250321 2.24m  2.24m   0.00743    1015MB    0.320     1    43      2.24m <dt>   <Rprofmem> 41,841

# latest distinct  1.55m  1.55m    0.0107    1022MB    0.569     1    53      1.55m <dt>   <Rprofmem> 43,146
# latest filter    1.85m  1.85m   0.00903     910MB    0.361     1    40      1.85m <dt>   <Rprofmem> 43147

#         distinct 1.84m  1.84m   0.00905     912MB    0.299     1    33      1.84m <dt>   <Rprofmem> 43129
#         filter   1.90m   1.9m   0.00878     910MB    0.316     1    36       1.9m <dt>   <Rprofmem> 43153



# 5 milhoes
# expression   min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     EMPATES
#            57.2m  57.2m  0.000292    7.84GB   0.0248     1    85      57.2m <dt>   <Rprofmem> 294,678



# checando empates no cadunico -------------------------------------------------


data.table::setDT(empate)

empate <- filter(cadgeo, empate==T)
empate <- empate[order(id)]
empate[, count := .N, by = id]
#  View(empate)

table(empate$tipo_resultado) / nrow(empate) * 100



d <- unique(empate$id)[1]

temp <- empate |>
  dplyr::filter(id == d ) #




temp <- empate |>
  dplyr::filter( tipo_resultado=='dn02') #

d <- unique(temp$id)[6]

temp <- temp |>
  dplyr::filter(id == d ) #

temp$tipo_resultado
temp$endereco_encontrado


mapview::mapview(temp, zcol='endereco_encontrado')

sf::st_distance(temp)

# nao era para ser empate
# 5 "da02" "da02"   mesmo rua e cep
# [1] "RUA PAULO SIMOES DA COSTA, 32 (aprox) - JARDIM ANGELA, SAO PAULO - SP, 04929-140"
# [2] "RUA PAULO SIMOES DA COSTA, 32 (aprox) - ALTO DO RIVIERA, SAO PAULO - SP, 04929-140"












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



# cad unico --------------------------------------------------------------------

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



