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
    cache = T,
    padronizar_enderecos = T
  )
)
