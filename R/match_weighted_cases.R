match_weighted_cases <- function(
  # nocov start
  con = con,
  x = 'input_padrao_db',
  output_tb = "output_db",
  key_cols = key_cols,
  match_type = match_type,
  resultado_completo
) {
  # match_type = "da01"
  # key_cols <- geocodebr:::get_key_cols(match_type)

  # get corresponding parquet table
  cnefe_table_name <- get_reference_table(match_type)
  y <- cnefe_table_name
  key_cols <- get_key_cols(match_type)

  # write cnefe table to db
  register_cnefe_table(con, match_type)

  # cols that cannot be null
  cols_not_null <- paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # remove numero from key cols to allow for the matching
  key_cols <- key_cols[key_cols != 'numero']

  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  # whether to keep all columns in the result
  colunas_encontradas <- ""
  additional_cols_first <- ""
  additional_cols_second <- ""

  if (isTRUE(resultado_completo)) {
    colunas_encontradas <- paste0(
      glue::glue("{key_cols}_encontrado"),
      collapse = ', '
    )

    colunas_encontradas <- gsub(
      'localidade_encontrado',
      'localidade_encontrada',
      colunas_encontradas
    )
    colunas_encontradas <- paste0(", ", colunas_encontradas)

    # additonal cols for the first part of the query
    additional_cols_first <- paste0(
      glue::glue("{y}.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', '
    )
    additional_cols_first <- gsub(
      'localidade_encontrado',
      'localidade_encontrada',
      additional_cols_first
    )
    additional_cols_first <- paste0(", ", additional_cols_first)

    # additonal cols for the second part of the query
    additional_cols_second <- paste0(
      glue::glue("FIRST({key_cols}_encontrado)"),
      collapse = ', '
    )
    additional_cols_second <- gsub(
      'localidade_encontrado',
      'localidade_encontrada',
      additional_cols_second
    )
    additional_cols_second <- paste0(", ", additional_cols_second)
  }

  # Match query  --------------------------------------------------------

  query_match <- glue::glue(
    "
    -- PART 1) left join to get all cases that match

    WITH temp_db AS (
      SELECT {x}.tempidgeocodebr,
             {x}.numero,
             {y}.numero AS numero_cnefe,
             {y}.lat, {y}.lon,
             REGEXP_REPLACE( {y}.endereco_completo, ', \\d+ -', CONCAT(', ', {x}.numero, ' (aprox) -')) AS endereco_encontrado,
             {y}.desvio_metros,
             {x}.log_causa_confusao,
             {y}.n_casos AS contagem_cnefe {additional_cols_first}
      FROM {x}
      INNER JOIN {y}
      ON {join_condition}
      WHERE {cols_not_null} AND {y}.lon IS NOT NULL
    )

    -- PART 2: aggregate and interpolate get aprox location
  INSERT INTO output_db (tempidgeocodebr, lat, lon, endereco_encontrado, tipo_resultado, desvio_metros, log_causa_confusao, contagem_cnefe {colunas_encontradas})
      SELECT tempidgeocodebr,
        SUM((1/ABS(numero - numero_cnefe) * lat)) / SUM(1/ABS(numero - numero_cnefe)) AS lat,
        SUM((1/ABS(numero - numero_cnefe) * lon)) / SUM(1/ABS(numero - numero_cnefe)) AS lon,
        FIRST(endereco_encontrado) AS endereco_encontrado,
        '{match_type}' AS tipo_resultado,
        AVG(desvio_metros) as desvio_metros,
        FIRST(log_causa_confusao) AS log_causa_confusao,
        FIRST(contagem_cnefe) AS contagem_cnefe {additional_cols_second}
    FROM temp_db
    GROUP BY tempidgeocodebr, endereco_encontrado;"
  )

  DBI::dbExecute(con, query_match)
  # b <- DBI::dbReadTable(con, 'output_db')
  # summary(b$desvio_metros)

  # b <- DBI::dbGetQuery(con, query_match)

  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
