# 1st step: create small table with unique logradouros
# 2nd step: update input_padrao_db with the most probable logradouro
# 3rd step: deterministic match
# 4th step: aggregate

match_weighted_cases_probabilistic <- function( # nocov start
  con = con,
  x = 'input_padrao_db',
  output_tb = "output_db",
  key_cols = key_cols,
  match_type = match_type,
  resultado_completo){

  # match_type = "pa01"

  # get corresponding parquet table
  cnefe_table_name <- get_reference_table(match_type)
  y <- cnefe_table_name
  key_cols <- get_key_cols(match_type)

  # write cnefe table to db
  register_cnefe_table(con, match_type)



  # 1st step: create small table with unique logradouros -----------------------
  unique_logradouros_tbl <- register_unique_logradouros_table(con, match_type)


  # 2nd step: update input_padrao_db with the most probable logradouro ---------

  calculate_string_dist(con, match_type, unique_logradouros_tbl)


  # 3rd step: match deterministico --------------------------------------------------------

  # cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  key_cols <- key_cols[ key_cols != 'numero']

  # Create the JOIN condition by concatenating the key columns
  join_condition_determ <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  # update join condition to use probable logradouro
  join_condition_determ <- gsub(
    'input_padrao_db.logradouro',
    'input_padrao_db.temp_lograd_determ',
    join_condition_determ
  )

  # cols that cannot be null
  cols_not_null_match <- gsub('.logradouro', '.temp_lograd_determ', cols_not_null)
  cols_not_null_match <- paste("AND ", cols_not_null_match)

  # whether to keep all columns in the result
  colunas_encontradas <- ""
  additional_cols_first <- ""
  additional_cols_second <- ""

  if (isTRUE(resultado_completo)) {

    colunas_encontradas <- paste0(
      glue::glue("{key_cols}_encontrado"),
      collapse = ', ')

    colunas_encontradas <- gsub('localidade_encontrado', 'localidade_encontrada', colunas_encontradas)
    colunas_encontradas <- paste0(", ", colunas_encontradas)

    # additonal cols for the first part of the query
    additional_cols_first <- paste0(
      glue::glue("{y}.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', ')
    additional_cols_first <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols_first)
    additional_cols_first <- paste0(", ", additional_cols_first)

    # additonal cols for the second part of the query
    additional_cols_second <- paste0(
      glue::glue("FIRST({key_cols}_encontrado) AS {key_cols}_encontrado"),
      collapse = ', ')
    additional_cols_second <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols_second)
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
             {x}.similaridade_logradouro,
             {y}.desvio_metros,
             {x}.log_causa_confusao,
             {y}.n_casos AS contagem_cnefe {additional_cols_first}
          FROM {x}
          LEFT JOIN {y}
          ON {join_condition_determ}
          WHERE lon IS NOT NULL {cols_not_null_match}
          )

  -- PART 2: aggregate and interpolate get aprox location

  INSERT INTO output_db (tempidgeocodebr, lat, lon, endereco_encontrado, tipo_resultado, desvio_metros,
                         log_causa_confusao, similaridade_logradouro, contagem_cnefe {colunas_encontradas})
       SELECT tempidgeocodebr,
         SUM((1/ABS(numero - numero_cnefe) * lat)) / SUM(1/ABS(numero - numero_cnefe)) AS lat,
         SUM((1/ABS(numero - numero_cnefe) * lon)) / SUM(1/ABS(numero - numero_cnefe)) AS lon,
         FIRST(endereco_encontrado) AS endereco_encontrado,
         '{match_type}' AS tipo_resultado,
         AVG(desvio_metros) AS desvio_metros,
         FIRST(log_causa_confusao) AS log_causa_confusao,
         FIRST(similaridade_logradouro) AS similaridade_logradouro,
         FIRST(contagem_cnefe) AS contagem_cnefe {additional_cols_second}
      FROM temp_db
      GROUP BY tempidgeocodebr, endereco_encontrado;"
  )


  DBI::dbExecute(con, query_match)
  # DBI::dbExecute(con, query_aggregate)
  # d <- DBI::dbReadTable(con, 'output_db')
  # d <- DBI::dbReadTable(con, 'aaa')




  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
