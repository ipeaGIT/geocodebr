#' proximos passos
#' 0. padronizacao cnefe usado aprox linear para lidar com ruas curvas
#' 1. add census tracts
#' 2. check ground truth data for distance
#' 3. more efficient string dist with cache
#' 4. make solving ties more efficient
#' 5. helper function to parallelize chuncks of the same size
#' 6. targets to track the evolution of the package performance
#'
#'
#'
# esperado
5 - deterministico certinho
7312 - OTIMO antes rua errada no bairro errada, agora pega rua certa no certo

# action
4398 - ! "RUA  DO BECO 12" continua com match probabilistico
1690 - ! v2 parece melhor erro na padronizacao???? enderecobr::padronizar_logradouros('RODOVIA  BR 364') 666666 checar como esta no cnfe e nas tabelas
9577 - ! v2 parece melhor erro na padronizacao???? enderecobr::padronizar_logradouros('RODOVIA  AC 475')
9470 - padronizacao???? enderecobr::padronizar_logradouros


15294 - ! antes pega rua certa no bairro certo (v2 melhor) ????

**** caso 1 - eh melhor o novo

# to ponder
463 -  1 antes pega rua errada no bairro certo, agora pega rua certa no bairro errado lol (muitos 7)
5000 - 1 antes pega rua errada no bairro certo, agora pega rua certa no bairro errado lol
7290 - 1 antes pega rua errada no bairro certo, agora pega rua certa no bairro errado lol (muitos empates)
8490 - 1 antes pega rua errada no bairro certo, agora pega rua certa no bairro errado lol
9808 - 1 antes pega rua errada no bairro certo, agora pega rua certa no bairro errado lol
423 -  1 antes pega rua errada no bairro certo, agora NAO pega rua mas acerta o bairro (desvio grande)
13814 - 1 antes pega rua certa no cep errado , agora NAO pega rua mas acerta o bairro (desvio 700)


5385 - 2 antes pega rua certa no bairro errado, agora NAO pega rua mas acerta o bairro (desvio grande)
7230 - 2 antes pega rua certa no bairro errado, agora NAO pega rua mas acerta o bairro (desvio 700)

13135 - v2 antes pega rua certa no bairro certo, agora NAO pega rua mas acerta o bairro (desvio 900)
14063 - nao tem salvacao, v3 melhor pq eh honesto com desvio grande
15734 - 3 antes pega rua certa no bairro errado, agora NAO pega rua mas acerta o bairro (desvio pequeno anyway)
13292 - 4 antes pega rua certa no bairro certo, agora NAO pega rua mas acerta o bairro (desvio pequeno anyway)
10714 - 4 antes pega rua certa no bairro certo, agora NAO pega rua mas acerta o bairro (desvio pequeno anyway)
9842 - 5 antes rua errada no bairro certo e cep errado, agora pega rua certa no bairro certo e cep errado (dificil)


333 - igual "RUA DOM PEDROII" - probabilistico
4998 - igual "RUA DOM PEDRO II" - deterministico

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
# h3_res = NULL

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


bench::mark(
  v3 <- geocode(
    enderecos = input_df,
    campos_endereco = campos,
     n_cores = 7,
    resultado_completo = F,
    verboso = T,
    # resultado_sf = T,
    resolver_empates = T,
    # h3_res = 9,
    cache= T, padronizar_enderecos = T
  )
)


gc(T,T,T)
bench::mark(

  iterations = 1, check = F,

  # callr = geocode_callr(
  #   enderecos = df,
  #   campos_endereco = campos,
  #   n_cores = 7,
  #   resultado_completo = F,
  #   resolver_empates = T
  #   ),

  original = geocode(
    enderecos = input_df,
    campos_endereco = campos,
    n_cores = 7,
    resultado_completo = F,
    resolver_empates = T
    )
)

#   expression     min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
#   <bch:expr> <bch:t> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>
# 1 original     24.3m  24.3m  0.000685    8.24GB  0.00479     1     7      24.3m <dt>   <Rprofmem>

#   expression     min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
# v0.4.0 CRAN     33.5m  33.5m  0.000497    8.06GB  0.00746     1    15      33.5m <NULL> <Rprofmem>
# v0.5.0 dev      22.2m  22.2m  0.000749    8.05GB  0.00674     1     9      22.2m <dt>   <Rprofmem>
# v0.5.0 devcallr 5.94m  5.94m  0.00280   1016.2MB  0           1     0      5.94m <NULL> <Rprofmem>


# args: n_cores = 7, resultado_completo = F, verboso = T, resultado_sf = T, resolver_empates = T, h3_res = 9
# expression      min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time       gc
# v0.4.0        28.6s  28.6s    0.0350      83MB    0.245     1     7      28.6s <sf>   <Rprofmem> <bench_tm> <tibble>
# v0.5.0_dev    7.63s  7.63s     0.131    39.4MB    0.918     1     7      7.63s <sf>   <Rprofmem> <bench_tm> <tibble>





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
# resultado_completo = F
# resolver_empates = T
# resultado_sf = FALSE
# h3_res =9

dfgeo <- geocodebr::geocode(
    enderecos = input_df,
    campos_endereco = campos,
    n_cores = 7,
    resultado_completo = T,
    resolver_empates = F,
    verboso = T
  )


identical(df_rafaF$id, input_df$id)


table(df_rafa_loop$precision) / nrow(df_rafa_loop)*100


unique(df_rafa$match_type) |> length()

table(df_rafa$match_type)


# parallel callr --------------------------------------

library(future.callr)
library(future)
library(furrr)

future::plan(future::multisession(workers = 3))

input_df$estado <- enderecobr::padronizar_estados(input_df$uf)




bench::bench_time(
a <-   split(input_df, f = input_df$estado) |>
    furrr::future_map(
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


future::plan(future.callr::callr)

bench::bench_time(
  split(input_df, f = "uf") |>
    furrr::future_map(
      .f = function(x){
        geocode_callr(
          enderecos = x,
          campos_endereco = campos,
          n_cores = 7,
          resultado_completo = F,
          resolver_empates = T
        )
      }
    )

)

bench::bench_time(
        geocode_callr(
          enderecos = df,
          campos_endereco = campos,
          n_cores = 10,
          resultado_completo = F,
          resolver_empates = T
        )
)

