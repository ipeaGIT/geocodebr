
register_cnefe_table <- function(con, match_type){

  # match_type = "dn01"

  # get corresponding table and key cols
  cnefe_table_name <- get_reference_table(match_type)

  # build path to local file
  files <- geocodebr::listar_dados_cache()
  path_to_parquet <- files[grepl( paste0(cnefe_table_name,".parquet"), files)]



  # ----------------------------------------------------------------------------
  # check if table already exists
  recorded_tbls <- duckdb::duckdb_list_arrow(conn = con)
  if (cnefe_table_name %in% recorded_tbls) {
    return(TRUE)
  }

  # determine geographical scope of the search
  input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado |>
    sort()

  input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio |>
    sort()

  # connect to parquet file
  cnefe_tbl <- arrow_open_dataset( path_to_parquet )

  # filter cnefe to include only states and municipalities
  if (length(input_municipio) < 5000 | length(input_states) < 27) {

    cnefe_tbl <- cnefe_tbl |>
      dplyr::filter(estado %in% input_states) |>
      dplyr::filter(municipio %in% input_municipio) |>
      dplyr::compute()
  }

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(
    conn = con,
    name =  cnefe_table_name,
    arrow_scannable = cnefe_tbl
    )

  # ----------------------------------------------------------------------------

  # # if table already exists, stop
  # if (duckdb::dbExistsTable(con, cnefe_table_name)) {
  #   return(TRUE)
  # }
  #
  # query_filter_cnefe <- glue::glue(
  #   "CREATE TEMP TABLE IF NOT EXISTS {cnefe_table_name} AS
  #         WITH unique_munis AS (
  #             SELECT DISTINCT municipio
  #             FROM input_padrao_db
  #         ),
  #         unique_states AS (
  #             SELECT DISTINCT estado
  #             FROM input_padrao_db
  #         )
  #         SELECT *
  #         FROM read_parquet('{path_to_parquet}') m
  #         WHERE m.municipio IN (SELECT municipio FROM unique_munis)
  #              AND m.estado IN (SELECT estado FROM unique_states);"
  #
  # )
  #
  # DBI::dbSendQueryArrow(con, query_filter_cnefe)

  return(TRUE)

}



# create small table with unique logradouros
register_unique_logradouros_table <- function(con, match_type){

  # match_type = "pn03"
  # get_reference_table(match_type)

  # create name of table with unique logradouros
  cnefe_table_name <- get_reference_table(match_type)
  cnefe_table_name <- gsub("_numero", "", cnefe_table_name)
  unique_logr_tbl_name <- paste0("unique_logr_", cnefe_table_name)
  unique_logr_tbl_parquet <- paste0(cnefe_table_name,".parquet")

  # check if table already exists
  recorded_tbls <- duckdb::duckdb_list_arrow(conn = con)
  if (unique_logr_tbl_name %in% recorded_tbls) {
    return(unique_logr_tbl_name)
  }

  # get corresponding key cols and unique cols to keep
  key_cols <- get_key_cols(match_type)
  unique_cols <- key_cols[!key_cols %in% "numero"]

  # tabela de referencia para unique logradouros
  files <- geocodebr::listar_dados_cache()
  path_to_parquet <- files[grepl(unique_logr_tbl_parquet, files)]


  # ----------------------------------------------------------------------------
  # determine geographical scope of the search
  input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado |>
    sort()
  input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio |>
    sort()

  # connect to parquet file
  unique_logradouros_arrw <- arrow_open_dataset( path_to_parquet )

  # filter unique logradouros to include only states and municipalities
  if (length(input_municipio) < 5000 | length(input_states) < 27) {

    unique_logradouros_arrw <- unique_logradouros_arrw |>
      dplyr::filter(estado %in% input_states) |>
      dplyr::filter(municipio %in% input_municipio) |>
      dplyr::compute()
  }

  duckdb::duckdb_register_arrow(
    conn = con,
    name = unique_logr_tbl_name,
    arrow_scannable = unique_logradouros_arrw
  )
  # ----------------------------------------------------------------------------

  # # if table already exists, stop
  # if (duckdb::dbExistsTable(con, unique_logr_tbl_name)) {
  #   return(unique_logr_tbl_name)
  # }
  #
  # query_unique_logradouros <- glue::glue(
  #   "CREATE TEMP TABLE IF NOT EXISTS {unique_logr_tbl_name} AS
  #         WITH unique_munis AS (
  #             SELECT DISTINCT municipio
  #             FROM input_padrao_db
  #         )
  #         SELECT DISTINCT {paste(unique_cols, collapse = ', ')}
  #         FROM read_parquet('{path_to_parquet}') m
  #         WHERE m.municipio IN (SELECT municipio FROM unique_munis);"
  #
  # )
  #
  # DBI::dbSendQueryArrow(con, query_unique_logradouros)

  return(unique_logr_tbl_name)

  # set index ?
  # key_cols <- get_key_cols(match_type)


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

