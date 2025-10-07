geocodebr::listar_dados_cache()
geocodebr::deletar_pasta_cache()

devtools::load_all('.')
library(dplyr)

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

bench::mark( iterations = 1,
             v3f <- geocodebr::geocode(
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
#           v2  28.7s  28.7s    0.0348    93.9MB    0.279     1     8      28.7s <dt>
#           v2  27.9s  27.9s    0.0358    50.8MB    0.250     1     7      27.9s <dt>
#           v3  33.3s  33.3s    0.0300    80.1MB    0.300     1    10      33.3s <dt>
#           v3  35.3s  35.3s    0.0283    1.53GB    0.198     1     7      35.3s <dt>
#           v3  36.7s  36.7s    0.0272    63.2MB    0.272     1    10      36.7s <dt>

#  sem desv v3  32.9s  32.9s    0.0304    1.26GB    0.213     1     7
#  com desv v3  31.7s  31.7s    0.0316    53.7MB    0.284     1     9      31.7s <dt>


v2 <- readRDS('out_v2.rds')
# 729 empates
# a base fica ao final com 22.363

# codigo novo na v2
# 730 empates
# a base fica ao final com 22.363
# > janitor::tabyl(v3f$empate)
#       empate     n   percent
#        FALSE 19298 0.8629433
#         TRUE  3065 0.1370567

# codigo novo na v3
v3 <- readRDS('out_v3.rds')
# 731-733 empates
# a base fica ao final com 22.370


#' TODO
#'
#' funcoes de cache que garantem versao no nome da pasta (copiar do censobr)
#'
#' lancar novo release v3 com desvio em integer
#'
#' a v3 esta dando muito mais empate
    # > table(v2$empate)
    #
    # FALSE  TRUE
    # 19299   729
    # > table(v3$empate)
    #
    # FALSE  TRUE
    # 19113  3591
identical(v2$precisao, v3$precisao)

table(v2$empate)
table(v3$empate)

v2 <- subset(v2, empate!=T)
v3 <- subset(v3, empate==T)

changers <- v2$id[v2$id %in% v3$id]

janitor::tabyl(subset(v2, id %in% changers)$precisao)
# subset(v2, id %in% changers)$precisao  n   percent
#                            logradouro 88 0.4705882
#                                numero 21 0.1122995
#                     numero_aproximado 78 0.4171123
janitor::tabyl(subset(v2, id %in% changers)$tipo_resultado)

changers_num <- subset(v3, precisao == 'numero')$id

changers_num <- setdiff(

  subset(v2, precisao == 'numero')$id,
  changers_num
  )


subset(v2, id %in% changers_num)$tipo_resultado |> table()



# teste de distancias -----------------




library(data.table)
library(mapview)
library(sf)
library(dplyr)

# open input data
data_path <- system.file("extdata/large_sample.parquet", package = "geocodebr")
input_df <- arrow::read_parquet(data_path)


geocodebr::listar_dados_cache()
geocodebr::deletar_pasta_cache()

ncores <- 7

campos <- geocodebr::definir_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  localidade = 'bairro',
  municipio = 'municipio',
  estado = 'uf'
)

bench::mark( iterations = 1,
             v3 <- geocodebr::geocode(
               enderecos = input_df,
               campos_endereco = campos,
               n_cores = ncores,
               resultado_completo = T,
               verboso = T,
               resultado_sf = F,
               resolver_empates = T
             )
)

# expression            min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
#   1 v2 <- geocodeb… 40.1s  40.1s    0.0249    3.08GB   0.0748     1     3      40.1s <dt>
#   1 v3 <- geocodeb… 54.7s  54.7s    0.0183    1.27GB    0.128     1     7      54.7s <dt>



# distancia
dt.haversine <- function(lat_from, lon_from, lat_to, lon_to, r = 6378137){
  radians <- pi/180
  lat_to <- lat_to * radians
  lat_from <- lat_from * radians
  lon_to <- lon_to * radians
  lon_from <- lon_from * radians
  dLat <- (lat_to - lat_from)
  dLon <- (lon_to - lon_from)
  a <- (sin(dLat/2)^2) + (cos(lat_from) * cos(lat_to)) * (sin(dLon/2)^2)
  return(2 * atan2(sqrt(a), sqrt(1 - a)) * r)
}

df <- dplyr::left_join(v2, v3, by='id')
data.table::setDT(df)
df[, dist  := dt.haversine(lat.x,lon.x, lat.y, lon.y) ]

summary(df$dist)
#   Min.   1st Qu.    Median      Mean   3rd Qu.      Max.
# 0.002     0.193     2.900   112.337    19.030 31080.604

# eh tudo muito proximo. Onde tem distancia muito grande,
# a grande maioria eh caso de empate de enderecos complicados
# entao o que causa a diferenca foi mais a solucao do empate em si
stats::quantile(df$dist, probs =c(.9, .95, .99))
#      90%       95%       99%
# 153.2783  507.0716 1811.1621




df2 <- df |> filter(dist > 1000)
i <- df2$id[10]

v2i <- sfheaders::sf_point(
  obj = subset(df2, id == i),
  x = 'lon.x',
  y = 'lat.x',
  keep = TRUE
)
v3i <- sfheaders::sf_point(
  obj = subset(df2, id == i),
  x = 'lon.y',
  y = 'lat.y',
  keep = TRUE
)
sf::st_crs(v2i) <- 4674
sf::st_crs(v3i) <- 4674

mapview::mapview(v2i, col.regions = "blue") +
  mapview(v3i, col.regions = "red")
v3i$precisao.x
v3i$precisao.y
v3i$endereco_encontrado.x
v3i$endereco_encontrado.y



janitor::tabyl(df2$empate.x)
