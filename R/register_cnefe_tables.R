
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
    dplyr::select(dplyr::all_of(c("estado", "municipio", "logradouro", "cep", "localidade"))) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    # dplyr::distinct() |>
    dplyr::compute()


  duckdb::duckdb_register_arrow(
    con,
    unique_logr_tbl_name,
    unique_logradouros_cep_localidade
    )

  return(TRUE)
}



calculate_string_dist <- function(con, match_type){

  # match_type = "pn01"

  cnefe_table_name <- get_reference_table(match_type)
  y <- cnefe_table_name
  key_cols <- get_key_cols(match_type)

  # cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("input_padrao_db.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # remove numero and logradouro from key cols to allow for the matching
  key_cols_string_dist <- key_cols[!key_cols %in%  c("numero", "logradouro")]

  join_condition_lookup <- paste(
    glue::glue("unique_logradouros.{key_cols_string_dist} = input_padrao_db.{key_cols_string_dist}"),
    collapse = ' AND '
  )

  # min cutoff for string match
  min_cutoff <- get_prob_match_cutoff(match_type)


  # query
  query_calc_dist <- glue::glue(
    "
    -- STEP 1: pick only rows that do NOT have similarity in temp table
    WITH to_compute AS (
      SELECT
          input_padrao_db.tempidgeocodebr,
          input_padrao_db.logradouro AS logradouro_input,
          unique_logradouros.logradouro AS logradouro_cnefe
      FROM input_padrao_db
      JOIN unique_logradouros
        ON {join_condition_lookup}
      WHERE {cols_not_null}
        AND input_padrao_db.log_causa_confusao = FALSE
        AND input_padrao_db.similaridade_logradouro IS NULL
        ),

    -- STEP 2: calculate similarity only for missing pairs
    computed AS (
      SELECT
          tempidgeocodebr,
          logradouro_cnefe,
          CAST(jaro_similarity(logradouro_input, logradouro_cnefe) AS NUMERIC(5,3)) AS similarity,
          RANK() OVER (PARTITION BY tempidgeocodebr ORDER BY similarity DESC, logradouro_cnefe) AS rank
      FROM to_compute
      WHERE similarity > {min_cutoff}
      )

    -- STEP 3: distances to input db
    UPDATE input_padrao_db
      SET temp_lograd_determ = computed.logradouro_cnefe,
          similaridade_logradouro = similarity
      FROM computed
      WHERE input_padrao_db.tempidgeocodebr = computed.tempidgeocodebr
            AND computed.rank = 1;"
  )


  DBI::dbSendQueryArrow(con, query_calc_dist)

  # a <- DBI::dbReadTable(con, 'input_padrao_db')
  # sum(is.na(a$similaridade_logradouro))
}

