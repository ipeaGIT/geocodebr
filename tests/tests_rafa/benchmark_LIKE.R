#' instalar extensoes do duckdb
#' - spatial - acho q nao vale a pena por agora
#'
#' #' (non-deterministic search)
#' - fts - Adds support for Full-Text Search Indexes / "https://medium.com/@havus.it/enhancing-database-search-full-text-search-fts-in-mysql-1bb548f4b9ba"
#'
#'
#'
#' adicionar dados de POI da Meta /overture
#' adicionar dados de enderecos Meta /overture
#'   # NEXT STEPS
#'   - (ok) interpolar numeros na mesma rua
#'   - (next) join probabilistico com fts_main_documents.match_bm25
#'   - (next) calcular nivel de erro na agregacao do cnefe
#'   - optimize disk and parallel operations in duckdb
#'   - casos de rodovias
#'   - interpolar numeros separando impares e pares
#'   - CASES NOT FOUND ? AND THEIR EFFECT ON THE PROGRESS BAR



#' take-away
#' 1) incluir LIKE no campo d elogradouro melhor MUITO performance, encontrando
#' muito mais casos em cases de match 1, 2, 3 e 4
#' o melhor mesmo seria usar fts
#' 2) isso tem pequeno efeito de diminuir performance do dani, e 0 efeito no rafa
#'
#' 3) no rafa aida tem um residudo de que alguns casos em que as coordenadas nao
#' foram agregadas, entao tem alguns 'id's que se repetem no output
#'  - a razao eh pq a agregacao sai diferente para logradouros diferentes mas
#'  com o mesmo padrao LIKE. Ex. "RUA AVELINO CAVALCANTE" e "TRAVESSA AVELINO CAVALCANTE"
#'
#'  exemplos
#' id == 1637 caso de diferentes ruas no mesmo condominio
#'            "RUA DOIS VILA RICA" e "RUA XXVI QUADRA E VILA RICA"
#' id == 1339 (esse se resolve pq sao bairros diferentes)


devtools::load_all('.')
library(dplyr)
# library(geocodebr)
# library(enderecobr)
# library(data.table)
# library(arrow)
# library(duckdb)


# open input data
data_path <- system.file("extdata/large_sample.parquet", package = "geocodebr")
input_df <- arrow::read_parquet(data_path)


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


# benchmark different approaches ------------------------------------------------------------------
ncores <- 7

# input_df <- input_df[c(1,84, 284), ]
# input_df <- input_df[id %in%  dfgeo$id[dfgeo$tipo_resultado=='ei03'] ]


campos <- geocodebr::definir_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  localidade = 'bairro',
  municipio = 'municipio',
  estado = 'uf'
)

# test probabilistic
# input_df <- input_df[c(7, 32, 34, 71, 173, 1348)]  # pn02 pi02 pn03 pi03 pr01 pr02
# temp_df <- filter(input_df,id %in% c(1371)  )


bench::mark( iterations = 1,
  temp_dfgeo2 <- geocodebr::geocode(
    enderecos = input_df,
    campos_endereco = campos,
    n_cores = ncores,
    resultado_completo = T,
    verboso = T,
    resultado_sf = F,
    resolver_empates = F
  )
)
# sequencia de matches
#   expression    min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time       gc
# 2nd best      25.7s  25.7s    0.0389    67.1MB    0.350     1     9      25.7s ;;787 empates
# ideal         23.8s  23.8s    0.0420    38.1MB    0.378     1     9      23.8s ;;724


# expression       min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
# <bch:expr>     <bch> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>
#   current      1.01m  1.01m    0.0165    81.3MB    0.182     1    11      1.01m <dt>   <Rprofmem>
#   ender_arrw   59.6s  59.6s    0.0168    81.2MB    0.201     1    12      59.6s <dt>   <Rprofmem>
#                40.2s  40.2s    0.0249      83MB    0.323     1    13      40.2s <dt>   <Rprofmem>
#                30.3s  30.3s    0.0330    71.8MB    0.363     1    11      30.3s <dt>   <Rprofmem> 715 empates
#                26.2s  26.2s    0.0382    70.3MB    0.343     1     9      26.2s <dt>   <Rprofmem> 722
#                24.5s  24.5s    0.0409    70.3MB    0.368     1     9      24.5s <dt>   <Rprofmem> 722
#                24.1s  24.1s    0.0414    63.8MB    0.373     1     9      24.1s <dt>   <Rprofmem> 704
# prob befo det  24.4s  24.4s    0.0411    39.3MB    0.411     1    10      24.4s <dt>   <Rprofmem> 723
# prob after det 24.9s  24.9s    0.0401      67MB    0.401     1    10      24.9s <dt>   <Rprofmem> 810
#                match prob
# filter               25.7s  25.7s    0.0389    63.8MB    0.350     1     9      25.7s <dt>   <Rprofmem> <bench_tm>
# distinct             27s    27s    0.0371    91.1MB    0.334     1     9        27s <dt>   <Rprofmem> <bench_tm>

# 20K prob numero pn
# atual:   46.69s / 706 empates
# old:     34.91s / 783
# 20250318 40.21s / 698 empates

# 1.56m com arrow distinct
# 1.47m com parquet no pn
# 1.90m com parquet no pn e pa
# 1.6 com write arrwo table   >>>>>>>>>>>> implementar com certeza
# 1.3m e arrow distinc
# 1.32m e arrow distinc  e outuop_arrw
# 1.06m e arrow distinc  e outuop_arrw e sendQueryArrow (na tomada)
# 1.06m isso + tudo com sendQueryArrow  (na tomada)



empates <- subset(dfgeo, empate==T)
a <- table(empates$id) |> as.data.frame()
table(empates$tipo_resultado)

case <- subset(empates, id ==9477)
case <- subset(empates, endereco_encontrado %like% 'BLOCO')
mapview::mapview(case)


round(table(dfgeo$precisao) / nrow(dfgeo) *100, 1)
round(table(dfgeo2$precisao) / nrow(dfgeo2) *100, 1)

# tempo:
# com 32.75s    855 empates
# sem 13.7s     655 empates

# precisao:
#       cep        localidade        logradouro         municipio            numero numero_aproximado
# com   2.5               0.3              14.8               0.6              45.8              36.0
# sem  31.4               2.3               7.2               0.6              33.3              26.2

temp <- left_join( select(dfgeo, id, tipo_resultado), select(dfgeo2, id, tipo_resultado), by = 'id'  )
t <- table(temp$tipo_resultado.x, temp$tipo_resultado.y) |> as.data.frame()
t <- filter(t, Freq>0)
View(t)





# 20K FRESH
# Unit: seconds
#         expr      min       lq     mean   median       uq      max neval
#    rafa_drop 10.10379 10.25397 10.35726 10.39609 10.41682 10.61564     5
# rafa_drop_db 10.33163 10.41716 10.53599 10.57100 10.67138 10.68880     5
#    rafa_keep 11.12560 11.17642 11.40483 11.30231 11.56706 11.85277     5
# rafa_keep_db 10.99093 11.18329 11.26758 11.18585 11.40816 11.56966     5
#      dani_df 10.99375 11.04987 11.47305 11.16092 11.43530 12.72541     5







# 20 K
#     expression      min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
#     <bch:expr>   <bch:> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>     <list>
#   1 rafa_drop     10.4s  10.5s    0.0952      53MB    0.171     5     9      52.5s <NULL> <Rprofmem> <bench_tm>
#   2 rafa_drop_db  10.6s  10.7s    0.0935    27.5MB    0.168     5     9     53.46s <NULL> <Rprofmem> <bench_tm>
#   3 rafa_keep     11.5s  11.7s    0.0859    27.7MB    0.155     5     9     58.23s <NULL> <Rprofmem> <bench_tm>
#   4 rafa_keep_db  11.3s  11.6s    0.0867    27.6MB    0.139     5     8     57.69s <NULL> <Rprofmem> <bench_tm>
#   5 dani_df       11.5s  11.9s    0.0823    24.9MB    0.148     5     9      1.01m <NULL> <Rprofmem> <bench_tm>





devtools::load_all('.')
library(tictoc)
library(dplyr)
library(data.table)
library(ipeadatalake)
library(mapview)
library(sfheaders)
library(sf)
options(scipen = 999)
mapview::mapviewOptions(platform = 'leafgl')
set.seed(42)





geocodebr::get_cache_dir() |>
  geocodebr:::arrow_open_dataset()  |>
  filter(estado=="PR") |>
  filter(municipio == "CURITIBA") |>
  dplyr::compute() |>
  filter(logradouro_sem_numero %like% "DESEMBARGADOR HUGO SIMAS") |>
  dplyr::collect()


cnf <- ipeadatalake::read_cnefe(year = 2022) |>
  #  filter(code_state=="41") |>
  dplyr::filter(code_muni == 4106902) |>
  dplyr::collect()


d <- cnf |>
  filter(nom_seglogr %like% "HUGO SIMAS")

'DESEMBARGADOR'






# small sample data ------------------------------------------------------------------
devtools::load_all('.')
library(dplyr)

# open input data
data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
input_df <- read.csv(data_path)

campos <- geocodebr::definir_campos(
  logradouro = "nm_logradouro",
  numero = "Numero",
  cep = "Cep",
  localidade = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf"
)

# enderecos = input_df
# campos_endereco = campos
# n_cores = 7
# verboso = T
# cache=T
# resultado_completo=T
# resolver_empates = FALSE
# resultado_sf = FALSE

dfgeo <- geocodebr::geocode(
    enderecos = input_df,
    campos_endereco = campos,
    n_cores = 7,
    resultado_completo = T,
    resolver_empates = T,
    verboso = T
  )


identical(df_rafaF$id, input_df$id)


table(df_rafa_loop$precision) / nrow(df_rafa_loop)*100


unique(df_rafa$match_type) |> length()

table(df_rafa$match_type)


