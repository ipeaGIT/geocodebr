devtools::load_all('.')

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


bench::mark(iterations = 5,
  a <- geocodebr::geocode(
    enderecos = input_df,
    campos_endereco = campos,
    n_cores = 7,
    resultado_completo = F,
    verboso = T,
    resultado_sf = F,
    resolver_empates = T,
    cache = T
  )
)

#          expression    min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
# streetmap 0.6.0 dev  7.51s  7.96s     0.127    2.03MB   0.0317     4     1      31.5s <df>   <Rprofmem>

