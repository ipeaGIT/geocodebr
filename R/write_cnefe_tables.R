
write_cnefe_tables <- function(con, match_type){

  # match_type = "dn01"

  # get corresponding table and key cols
  cnefe_table_name <- get_reference_table(match_type)

  # check if table already exists
  recorded_tbls <- DBI::dbListTables(con)
  if (cnefe_table_name %in% recorded_tbls) {
    return(TRUE)
    }

  files <- geocodebr::listar_dados_cache()
  path_to_parquet <- files[grepl( paste0(cnefe_table_name,".parquet"), files)]

  query_unique_logradouros <- glue::glue(
    "CREATE TEMP TABLE IF NOT EXISTS {cnefe_table_name} AS
          WITH unique_munis AS (
              SELECT DISTINCT municipio
              FROM input_padrao_db
          )
          SELECT *
          FROM read_parquet('{path_to_parquet}') m
          WHERE m.municipio IN (SELECT municipio FROM unique_munis);"

  )
  DBI::dbSendQueryArrow(con, query_unique_logradouros)
  # DBI::dbExecute(con, query_unique_logradouros)

  return(TRUE)

  # set index ?
  key_cols <- get_key_cols(match_type)

  # if (cnefe_table_name %in c(tabelas mais pesadas)) {
  #   criar index
  #
  # }

  }






# create small table with unique logradouros
write_unique_logradouros_tables <- function(con, match_type){

  # match_type = "dn01"

  # create name of table with unique logradouros
  cnefe_table_name <- get_reference_table(match_type)
  unique_logr_tbl_name <- paste0("unique_logr_", cnefe_table_name)

  # check if table already exists
  recorded_tbls <- DBI::dbListTables(con)
  if (unique_logr_tbl_name %in% recorded_tbls) {
    return(TRUE)
  }

  # get corresponding key cols
  key_cols <- get_key_cols(match_type)
  unique_cols <- key_cols[!key_cols %in% "numero"]

  # tabela de referencia para unique logradouros
  files <- geocodebr::listar_dados_cache()
  path_to_parquet_mlcl <- files[grepl("municipio_logradouro_cep_localidade.parquet", files)]

  query_unique_logradouros <- glue::glue(
    "CREATE TEMP TABLE IF NOT EXISTS {unique_logr_tbl_name} AS
          WITH unique_munis AS (
              SELECT DISTINCT municipio
              FROM input_padrao_db
          )
          SELECT DISTINCT {paste(unique_cols, collapse = ', ')}
          FROM read_parquet('{path_to_parquet_mlcl}') m
          WHERE m.municipio IN (SELECT municipio FROM unique_munis);"

  )

  DBI::dbSendQueryArrow(con, query_unique_logradouros)
  # DBI::dbExecute(con, query_unique_logradouros)

  return(TRUE)

  # set index ?
  key_cols <- get_key_cols(match_type)

}

# 43 milhoes
# expression   min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
# send        4.5h   4.5h 0.0000617    36.9GB   0.0105     1   171       4.5h <NULL> <Rprofmem>
# execute


#
# create_index(
#   con = con,
#   tbl_name,
#   cols,
#   index_type = "BTREE"
# )
#
#
# create_index <- function(
#     con,
#     tbl_name,
#     cols,
#     index_type = "BTREE" # BTREE RTREE
# ) {
#
#   cols <- strsplit(x = tbl_name, "_") |> unlist()
#
#   # index name
#   idx_name <- paste0(
#     "idx_",
#     tbl_name
#   )
#
#   # build column expression
#   cols_expr <- paste(cols, collapse = ", ")
#
#
#   if (index_type == "BTREE") {
#
#     sql <- glue::glue("
#       CREATE INDEX IF NOT EXISTS {idx_name}
#       ON {tbl_name} ({cols_expr});
#     ")
#
#   } else if (index_type == "RTREE") {
#
#     # RTREE index requires DuckDB spatial extension
#     sql <- glue::glue("
#       CREATE INDEX IF NOT EXISTS {idx_name}
#       ON {tbl_name}
#       USING RTREE ({cols_expr});
#     ")
#
#   }
#
#   tryCatch(
#     DBI::dbExecute(con, sql),
#     error = function(e) {
#       message("create_index(): failed to create index ", idx_name,
#               " on table ", tbl_name, " - ", e$message)
#     }
#   )
#
#   return(invisible(TRUE))
#
#
# }
