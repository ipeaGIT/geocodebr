
register_cnefe_table <- function(con, match_type){

  # match_type = "dn01"

  # get corresponding table and key cols
  cnefe_table_name <- get_reference_table(match_type)

  # check if table already exists
  recorded_tbls <- duckdb::duckdb_list_arrow(conn = con)
  if (cnefe_table_name %in% recorded_tbls) {
    return(TRUE)
  }

  # build path to local file
  files <- geocodebr::listar_dados_cache()
  path_to_parquet <- files[grepl( paste0(cnefe_table_name,".parquet"), files)]

  # determine geographical scope of the search
  input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado
  input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio

  # filter cnefe to include only states and municipalities
  filtered_cnefe <- arrow_open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, cnefe_table_name, filtered_cnefe)


  return(TRUE)

}




# create small table with unique logradouros
register_unique_logradouros_table <- function(con, match_type){

  # match_type = "pn01"

  # create name of table with unique logradouros
  unique_logr_tbl_name <- "unique_logradouros"

  # check if table already exists
  recorded_tbls <- duckdb::duckdb_list_arrow(conn = con)
  if (unique_logr_tbl_name %in% recorded_tbls) {
    return(TRUE)
  }


  # tabela de referencia para unique logradouros
  files <- geocodebr::listar_dados_cache()
  path_to_parquet_mlcl <- files[grepl("municipio_logradouro_cep_localidade.parquet", files)]


  # determine geographical scope of the search
  input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado
  input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio


  # tabela de referencia para unique logradouros
  unique_logradouros_cep_localidade <- arrow_open_dataset( path_to_parquet_mlcl ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()


  duckdb::duckdb_register_arrow(
    con,
    unique_logr_tbl_name,
    unique_logradouros_cep_localidade
    )

  return(TRUE)
}
