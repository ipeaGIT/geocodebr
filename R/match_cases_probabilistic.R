# prolema atual é que a pacote recalcula distancias de string TODA VEZ
# seria melhor criar uma tabela de distancia e ir populando

# 1st step: create small table with unique logradouros
# 2nd step: update input_padrao_db with the most probable logradouro
# 3rd step: deterministic match to update output

match_cases_probabilistic <- function(
    con = con,
    x = 'input_padrao_db',
    output_tb = "output_db",
    key_cols = key_cols,
    match_type = match_type,
    resultado_completo){ # nocov start

  # match_type = "pn01"

  # get corresponding parquet table and key columns
  cnefe_table_name <- get_reference_table(match_type)
  y <- cnefe_table_name
  key_cols <- get_key_cols(match_type)

  # write cnefe table to db
  write_cnefe_tables(con, match_type)


  # 1st step: create small table with unique logradouros -----------------------
  unique_logr_tbl_name <- paste0("unique_logr_", cnefe_table_name)
  write_unique_logradouros_tables(con, match_type)



  # 2nd step: update input_padrao_db with the most probable logradouro ---------

  # cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # remove numero and logradouro from key cols to allow for the matching
  key_cols_string_dist <- key_cols[!key_cols %in%  c("numero", "logradouro")]

  join_condition_lookup <- paste(
    glue::glue("{unique_logr_tbl_name}.{key_cols_string_dist} = {x}.{key_cols_string_dist}"),
    collapse = ' AND '
  )

  # min cutoff for string match
  min_cutoff <- get_prob_match_cutoff(match_type)

  # query
  query_lookup <- glue::glue(
    "WITH ranked_data AS (
        SELECT
          {x}.tempidgeocodebr,
          {unique_logr_tbl_name}.logradouro AS logradouro_cnefe,
          CAST(jaro_similarity({x}.logradouro, {unique_logr_tbl_name}.logradouro) AS NUMERIC(5,3)) AS similarity,
          RANK() OVER (PARTITION BY {x}.tempidgeocodebr ORDER BY similarity DESC, logradouro_cnefe) AS rank
        FROM {x}
        JOIN {unique_logr_tbl_name}
          ON {join_condition_lookup}
       WHERE {cols_not_null}
             AND {x}.log_causa_confusao is false
             AND {x}.similaridade_logradouro IS NULL
             AND similarity > {min_cutoff}
      )

      UPDATE {x}
         SET temp_lograd_determ = ranked_data.logradouro_cnefe,
             similaridade_logradouro = similarity
       FROM ranked_data
      WHERE {x}.tempidgeocodebr = ranked_data.tempidgeocodebr
            AND ranked_data.similarity > {min_cutoff}
            AND ranked_data.rank = 1;"
  )



  DBI::dbSendQueryArrow(con, query_lookup)
  # DBI::dbExecute(con, query_lookup)
  # b <- DBI::dbReadTable(con, 'input_padrao_db')




  # 3rd step: update output table com match deterministico --------------------------------------------------------

  key_cols <- get_key_cols(match_type)

  # update join condition to use probable logradouro and deterministic number
  join_condition_match <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  join_condition_match <- gsub('input_padrao_db.logradouro', 'input_padrao_db.temp_lograd_determ', join_condition_match)

  # update cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # cols that cannot be null
  cols_not_null <- gsub('.logradouro', '.temp_lograd_determ', cols_not_null)

  # whether to keep all columns in the result
  colunas_encontradas <- ""
  additional_cols <- ""


  if (isTRUE(resultado_completo)) {

    colunas_encontradas <- paste0(
      glue::glue("{key_cols}_encontrado"),
      collapse = ', ')

    colunas_encontradas <- gsub('localidade_encontrado', 'localidade_encontrada', colunas_encontradas)
    colunas_encontradas <- paste0(", ", colunas_encontradas)
    colunas_encontradas <- paste0(colunas_encontradas, ", similaridade_logradouro")

    additional_cols <- paste0(
      glue::glue("{y}.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols, ", input_padrao_db.similaridade_logradouro AS similaridade_logradouro")
  }


  # summarize query
  query_update_db <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, endereco_encontrado, tipo_resultado,
                            desvio_metros, log_causa_confusao, contagem_cnefe {colunas_encontradas} )
      SELECT {x}.tempidgeocodebr,
        {y}.lat,
        {y}.lon,
        {y}.endereco_completo AS endereco_encontrado,
        '{match_type}' AS tipo_resultado,
        {y}.desvio_metros,
        {x}.log_causa_confusao,
        {y}.n_casos AS contagem_cnefe {additional_cols}
      FROM {x}
      LEFT JOIN {y}
      ON {join_condition_match}
      WHERE {cols_not_null} AND {y}.lon IS NOT NULL;"
  )



  DBI::dbSendQueryArrow(con, query_update_db)
  # DBI::dbExecute(con, query_update_db)
  # c <- DBI::dbReadTable(con, 'output_db')


  # # remove arrow tables from db
  # duckdb::duckdb_unregister_arrow(con, "unique_logradouros")
  # duckdb::duckdb_unregister_arrow(con, cnefe_table_name) # 6666666666666666666



  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
