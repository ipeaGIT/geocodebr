


register_cnefe_table <- function(con, match_type){

  # message("register_cnefe_table")

  # match_type = "dn01"

  # get corresponding table and key cols
  cnefe_table_name <- get_reference_table(match_type)

  # build path to local file
  files <- geocodebr::listar_dados_cache()
  path_to_parquet <- files[grepl( paste0(cnefe_table_name,".parquet"), files)]



  # # ----------------------------------------------------------------------------
  # # check if table already exists
  # recorded_tbls <- duckdb::duckdb_list_arrow(conn = con)
  # if (cnefe_table_name %in% recorded_tbls) {
  #   return(TRUE)
  # }
  #
  # # determine geographical scope of the search
  # input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado |>
  #   sort()
  #
  # input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio |>
  #   sort()
  #
  # # connect to parquet file
  # cnefe_tbl <- arrow_open_dataset( path_to_parquet )
  #
  # # filter cnefe to include only states and municipalities
  # if (length(input_municipio) < 5000 | length(input_states) < 27) {
  #
  #   cnefe_tbl <- cnefe_tbl |>
  #     dplyr::filter(estado %in% input_states) |>
  #     dplyr::filter(municipio %in% input_municipio) |>
  #     dplyr::compute()
  # }
  #
  # # register filtered_cnefe to db
  # duckdb::duckdb_register_arrow(
  #   conn = con,
  #   name =  cnefe_table_name,
  #   arrow_scannable = cnefe_tbl
  #   )
  #
  # # ----------------------------------------------------------------------------

  # if table already exists, stop
  if (duckdb::dbExistsTable(con, cnefe_table_name)) {
    return(TRUE)
  }

  query_filter_cnefe <- glue::glue(
    "CREATE TEMP TABLE IF NOT EXISTS {cnefe_table_name} AS
          WITH unique_munis AS (
              SELECT DISTINCT municipio
              FROM input_padrao_db
          ),
          unique_states AS (
              SELECT DISTINCT estado
              FROM input_padrao_db
          )
          SELECT *
          FROM read_parquet('{path_to_parquet}') m
          WHERE m.municipio IN (SELECT municipio FROM unique_munis)
               AND m.estado IN (SELECT estado FROM unique_states);"

  )

  DBI::dbExecute(con, query_filter_cnefe)

  return(TRUE)

}



# create small table with unique logradouros
register_unique_logradouros_table <- function(con, match_type){


  # match_type = "pn03"
  # get_reference_table(match_type)

  # get corresponding key cols and unique cols to keep
  key_cols <- get_key_cols(match_type)

  # determine reference table
  cnefe_table_name <- ifelse(
    match_type %in% c("pn03", "pa03", "pl03"),
    "municipio_logradouro_localidade",
    "municipio_logradouro_cep_localidade"
    )

  # create name of table with unique logradouros
  unique_logr_tbl_name <- paste0("unique_logr_", cnefe_table_name)

  # cols to keep unique values
  select_cols <- key_cols[!key_cols %in% c("numero")]


  # path to parquet
  unique_logr_tbl_parquet <- paste0(cnefe_table_name,".parquet")
  files <- geocodebr::listar_dados_cache()
  path_to_parquet <- files[grepl(unique_logr_tbl_parquet, files)]

  # should use DISTINCT rows
  DISTINCT <- "DISTINCT"
  if (cnefe_table_name=="municipio_logradouro_localidade" |
      all(c("localidade", "cep") %in% select_cols)
      ){
    DISTINCT <- ""
    }


  # # ----------------------------------------------------------------------------
  # # check if table already exists
  # recorded_tbls <- duckdb::duckdb_list_arrow(conn = con)
  # if (unique_logr_tbl_name %in% recorded_tbls) {
  #   return(unique_logr_tbl_name)
  # }
  #
  # # determine geographical scope of the search
  # input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado |>
  #   sort()
  # input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio |>
  #   sort()
  #
  # # connect to parquet file
  # unique_logradouros_arrw <- arrow_open_dataset( path_to_parquet )
  #
  #
  # # filter unique logradouros to include only states and municipalities
  # if (length(input_municipio) < 5000 | length(input_states) < 27) {
  #
  #   unique_logradouros_arrw <- unique_logradouros_arrw |>
  #     dplyr::filter(estado %in% input_states) |>
  #     dplyr::filter(municipio %in% input_municipio) |>
  #     dplyr::compute()
  # }
  #
  # if (DISTINCT=="DISTINCT") {
  #   unique_logradouros_arrw <- unique_logradouros_arrw |>
  #     dplyr::select(dplyr::all_of(select_cols))
  #     dplyr::distinct() |>
  #     dplyr::compute()
  # }
  #
  #
  # duckdb::duckdb_register_arrow(
  #   conn = con,
  #   name = unique_logr_tbl_name,
  #   arrow_scannable = unique_logradouros_arrw
  # )
  # # ----------------------------------------------------------------------------

  # ainda dah pra fazer (1) manter apenas as unique_cols, fazer filtro apenas quando
  # forem poucos munis < 5000

  # if table already exists, stop
  if (duckdb::dbExistsTable(con, unique_logr_tbl_name)) {
    return(unique_logr_tbl_name)
  }

 select_cols <- paste(select_cols, collapse = ', ')

  # se tabela raiz ja existe, filtra dela
  if (duckdb::dbExistsTable(con, cnefe_table_name)) {

    query_unique_logradouros <- glue::glue(
      "CREATE TEMP TABLE IF NOT EXISTS {unique_logr_tbl_name} AS
          WITH unique_munis AS (
              SELECT DISTINCT municipio
              FROM input_padrao_db
          )

          SELECT {DISTINCT} {select_cols}
            FROM {cnefe_table_name}
            WHERE {cnefe_table_name}.municipio IN (SELECT municipio FROM unique_munis);"
    )


  # caso contrario, leia o parquet
  } else {

    query_unique_logradouros <- glue::glue(
      "CREATE TEMP TABLE IF NOT EXISTS {unique_logr_tbl_name} AS
            WITH unique_munis AS (
                SELECT DISTINCT municipio
                FROM input_padrao_db
            )

          SELECT {DISTINCT} {select_cols}
              FROM read_parquet('{path_to_parquet}') m
              WHERE m.municipio IN (SELECT municipio FROM unique_munis);"
    )
  }
  DBI::dbExecute(con, query_unique_logradouros)




  # set index ?
  # key_cols <- get_key_cols(match_type)

  return(unique_logr_tbl_name)

}



calculate_string_dist <- function(con, match_type, unique_logradouros_tbl){

  # message("calculate_string_dist")



  # match_type = "pl01"

  key_cols <- get_key_cols(match_type)

  # cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("input_padrao_db.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # remove numero and logradouro from key cols to allow for the matching
  key_cols_string_dist <- key_cols[!key_cols %in%  c("numero", "logradouro")]

  join_condition_lookup <- paste(
    glue::glue("{unique_logradouros_tbl}.{key_cols_string_dist} = input_padrao_db.{key_cols_string_dist}"),
    collapse = ' AND '
  )

  # min cutoff for string match
  min_cutoff <- get_prob_match_cutoff(match_type)


  #-----------------------------------------------------------------------------

  # query novo
  query_calc_dist <- glue::glue(
    "
    -- STEP 1: pick only rows that do NOT have similarity in temp table
    WITH to_compute AS (
      SELECT
          input_padrao_db.tempidgeocodebr,
          input_padrao_db.logradouro AS logradouro_input,
          {unique_logradouros_tbl}.logradouro AS logradouro_cnefe
      FROM input_padrao_db
      JOIN {unique_logradouros_tbl}
        ON {join_condition_lookup}
      WHERE input_padrao_db.similaridade_logradouro IS NULL
        AND input_padrao_db.log_causa_confusao = FALSE
        AND {cols_not_null}
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


  DBI::dbExecute(con, query_calc_dist)
  #-----------------------------------------------------------------------------



  # # query antigo
  # query_lookup <- glue::glue(
  #   "WITH ranked_data AS (
  #       SELECT
  #         input_padrao_db.tempidgeocodebr,
  #         {unique_logradouros_tbl}.logradouro AS logradouro_cnefe,
  #         CAST(jaro_similarity(input_padrao_db.logradouro, {unique_logradouros_tbl}.logradouro) AS NUMERIC(5,3)) AS similarity,
  #         RANK() OVER (PARTITION BY input_padrao_db.tempidgeocodebr ORDER BY similarity DESC, logradouro_cnefe) AS rank
  #       FROM input_padrao_db
  #       JOIN {unique_logradouros_tbl}
  #         ON {join_condition_lookup}
  #      WHERE {cols_not_null}
  #            AND input_padrao_db.log_causa_confusao is false
  #            AND input_padrao_db.similaridade_logradouro IS NULL
  #            AND similarity > {min_cutoff}
  #     )
  #
  #     UPDATE input_padrao_db
  #        SET temp_lograd_determ = ranked_data.logradouro_cnefe,
  #            similaridade_logradouro = similarity
  #      FROM ranked_data
  #     WHERE input_padrao_db.tempidgeocodebr = ranked_data.tempidgeocodebr
  #           AND ranked_data.similarity > {min_cutoff}
  #           AND ranked_data.rank = 1;"
  #     )
  #
  # DBI::dbExecute(con, query_lookup)



  # a <- DBI::dbReadTable(con, 'input_padrao_db')
  # sum(is.na(a$similaridade_logradouro))
}




# write_all_cnefe_tables_to_db <- function(con){
#
#   all_tables <- c("municipio_logradouro_numero_cep_localidade",
#                   "municipio_logradouro_numero_localidade",
#                   "municipio_logradouro_cep_localidade",
#                   "municipio_logradouro_localidade",
#                   "municipio_cep_localidade",
#                   "municipio_cep",
#                   "municipio_localidade",
#                   "municipio"
#   )
#
#
#   # build path to local file
#   files <- geocodebr::listar_dados_cache()
#
#
#   for (i in all_tables){
#
#     path_to_parquet <- files[grepl( paste0(i, ".parquet"), files)]
#
#     query_filter_cnefe <- glue::glue(
#       "CREATE TEMP TABLE {i} AS
#           WITH unique_munis AS (
#               SELECT DISTINCT municipio
#               FROM input_padrao_db
#           ),
#           unique_states AS (
#               SELECT DISTINCT estado
#               FROM input_padrao_db
#           )
#           SELECT *
#           FROM read_parquet('{path_to_parquet}') m
#           WHERE m.municipio IN (SELECT municipio FROM unique_munis)
#                AND m.estado IN (SELECT estado FROM unique_states);"
#
#     )
#
#     DBI::dbExecute(con, query_filter_cnefe)
#   }
#
# }


