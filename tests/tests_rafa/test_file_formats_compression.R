library(dplyr)
library(mapview)
library(sfheaders)
library(data.table)
library(geocodebr)
library(pbapply)
library(mirai)

# generating compressed files --------------------------------------------------

convert_format <- function(i){ # i=1

  file_parquet_original <- geocodebr::listar_dados_cache()[i]

  arrw_cnefe <- arrow::open_dataset( file_parquet_original )

  fname <- basename(file_parquet_original)

  arrow::write_parquet(
    x = arrw_cnefe,
    sink = fname,
    compression='zstd',
    compression_level = 22) #22

  arrow::write_ipc_file(
    x = arrw_cnefe,
    sink = gsub(".parquet", "_ipc" ,fname),
    compression='zstd',
    compression_level = 22)
}

#  pbapply::pblapply(X = 1:8, FUN = convert_format)


# PARALLEL WITH MIRAI
ncores <- 8
mirai::daemons(ncores)

results <- mirai::mirai_map(
  .x = 1:8,
  .f = function(i){ # i=1

    file_parquet_original <- geocodebr::listar_dados_cache()[i]

    arrw_cnefe <- arrow::open_dataset( file_parquet_original )

    fname <- basename(file_parquet_original)

    arrow::write_parquet(
      x = arrw_cnefe,
      sink = fname,
      compression='zstd',
      compression_level = 22)

    arrow::write_ipc_file(
      x = arrw_cnefe,
      sink = gsub(".parquet", "_ipc" ,fname),
      compression='zstd',
      compression_level = 22)

    return(i)
    }
  )[.progress]





# checar se todos arquivos estao ok
check_number_rows_parquet <- function(f){
  con <- arrow::open_dataset(f)
  return(nrow(con))
}

check_number_rows_ipc <- function(f){
  con <- arrow::open_dataset(f, format = 'ipc')
  return(nrow(con))
}

files_ipc <- list.files("C:/Users/r1701707/Desktop/geo_ipc/", full.names = T)
files_ori <- list.files("C:/Users/r1701707/Desktop/geo_original/", full.names = T)
files_cmp <- list.files("C:/Users/r1701707/Desktop/geo_compressed/", full.names = T)

bench::mark(
  ipc = lapply(X = files_ipc , FUN = check_number_rows_ipc) |> unlist() |> sum(),
  ori = lapply(X = files_ori , FUN = check_number_rows_parquet) |> unlist() |> sum(),
  cmp = lapply(X = files_cmp , FUN = check_number_rows_parquet) |> unlist() |> sum(), check = T
)

# expression      min   median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result    memory               time       gc
# 1 ipc       712.2ms  712.2ms      1.40   574.6KB     0        1     0      712ms <int [1]> <Rprofmem [301 × 3]> <bench_tm [1]>  <tibble>
# 2 ori        36.9ms   40.2ms     24.6     46.9KB     2.23    11     1      448ms <int [1]> <Rprofmem [185 × 3]> <bench_tm [12]> <tibble>
# 3 cmp        34.7ms   42.3ms     23.6     35.5KB     2.14    11     1      466ms <int [1]> <Rprofmem [152 × 3]> <bench_tm [12]> <tibble>





# comparing time performance  --------------------------------------------------

devtools::load_all('.')
library(dplyr)

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


pasta_ipc <- "C:/Users/r1701707/Desktop/geo_ipc/"
pasta_ori <- "C:/Users/r1701707/Desktop/geo_original/"
pasta_cmp <- "C:/Users/r1701707/Desktop/geo_compressed/"

geocodebr::definir_pasta_cache(pasta_ipc)
geocodebr::listar_pasta_cache()

bench::mark( iterations =5,
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


# expression   min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time       gc
#   original 26.2s  26.2s    0.0381    62.6MB    0.343     1     9      26.2s <dt>   <Rprofmem> <bench_tm> <tibble>
#   original 24.9s  25.4s    0.0393    65.5MB    0.385     5    49      2.12m <dt>   <Rprofmem> <bench_tm> <tibble>
# compressed 26.6s  26.6s    0.0376      66MB    0.338     1     9      26.6s <dt>   <Rprofmem> <bench_tm> <tibble>
# compressed 27.7s  28.1s    0.0346    38.4MB    0.166     5    24      2.41m <dt>   <Rprofmem> <bench_tm> <tibble>
# ipc        31.9s  31.9s    0.0313    56.9MB    0.345     1    11      31.9s <dt>   <Rprofmem> <bench_tm> <tibble>
# ipc        31.2s  31.9s    0.0307    38.6MB    0.252     5    41      2.71m <dt>   <Rprofmem> <bench_tm> <tibble>


