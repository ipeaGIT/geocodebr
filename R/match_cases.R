match_cases <- function( # nocov start
  con = con,
  x = "input_padrao_db",
  output_tb = "output_db",
  key_cols = key_cols,
  match_type = match_type,
  resultado_completo){

  # match_type = "dn01"

  # get corresponding table and key cols
  y <- get_reference_table(match_type)
  key_cols <- get_key_cols(match_type)

  # write cnefe table to db
  write_cnefe_tables(con, match_type)

  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  # cols from x that cannot be null
  # isso serve como filtro pre-join, pra fazer o join soh em quem nao foi encontrado ainda
  cols_not_null <- paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # whether to keep all columns in the result
  colunas_encontradas <- ""
  additional_cols <- ""

  if (isTRUE(resultado_completo)) {

    colunas_encontradas <- paste0(
      glue::glue("{key_cols}_encontrado"),
      collapse = ', ')

    colunas_encontradas <- gsub('localidade_encontrado', 'localidade_encontrada', colunas_encontradas)
    colunas_encontradas <- paste0(", ", colunas_encontradas)

    additional_cols <- paste0(
      glue::glue("{y}.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols)

  }

  # summarize query
  query_match <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, endereco_encontrado, tipo_resultado,
                            desvio_metros, log_causa_confusao, contagem_cnefe {colunas_encontradas})
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
      ON {join_condition}
      WHERE {cols_not_null} AND {y}.lon IS NOT NULL;"
  )

  DBI::dbSendQueryArrow(con, query_match)
  # a <- DBI::dbReadTable(con, 'output_db')
  # summary(a$desvio_metros)
  # summary(a$lat)

  #### 66666 remover
  # duckdb::duckdb_unregister_arrow(con, cnefe_table_name)

  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
