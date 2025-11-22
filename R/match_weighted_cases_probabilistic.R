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




  # 1st step: create small table with unique logradouros -----------------------


  # 666 esse passo poderia tmb filtar estados e municipios presentes
  unique_cols <- key_cols[!key_cols %in% "numero"]
  aprox_tbl_name <- paste0("aprox_", paste0(unique_cols,collapse = "_" ))

  files <- geocodebr::listar_dados_cache()
  path_to_parquet_mlcl <- files[grepl("municipio_logradouro_cep_localidade.parquet", files)]

  query_unique_logradouros <- glue::glue(
    "CREATE TABLE IF NOT EXISTS {aprox_tbl_name} AS
          WITH unique_munis AS (
              SELECT DISTINCT municipio
              FROM input_padrao_db
          )
          SELECT DISTINCT {paste(unique_cols, collapse = ', ')}
          FROM read_parquet('{path_to_parquet_mlcl}') m
          WHERE m.municipio IN (SELECT municipio FROM unique_munis);"

  )

  DBI::dbSendQueryArrow(con, query_unique_logradouros)



  # 2nd step: update input_padrao_db with the most probable logradouro ---------

  # cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # remove numero and logradouro from key cols to allow for the matching
  key_cols_string_dist <- key_cols[!key_cols %in%  c("numero", "logradouro")]

  join_condition_string_dist <- paste(
    glue::glue("{aprox_tbl_name}.{key_cols_string_dist} = {x}.{key_cols_string_dist}"),
    collapse = ' AND '
  )

  # min cutoff for string match
  min_cutoff <- get_prob_match_cutoff(match_type)

  # query update input table with probable logradouro
  query_lookup <- glue::glue(
    "WITH ranked_data AS (
        SELECT
          {x}.tempidgeocodebr,
          {aprox_tbl_name}.logradouro AS logradouro_cnefe,
          CAST(jaro_similarity({x}.logradouro, {aprox_tbl_name}.logradouro) AS NUMERIC(5,3)) AS similarity,
          RANK() OVER (PARTITION BY {x}.tempidgeocodebr ORDER BY similarity DESC, logradouro_cnefe) AS rank
        FROM {x}
        JOIN {aprox_tbl_name}
          ON {join_condition_string_dist}
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
            AND ranked_data.rank = 1
            AND ranked_data.similarity > {min_cutoff};"
  )

  DBI::dbSendQueryArrow(con, query_lookup)
  # DBI::dbExecute(con, query_lookup)
  # b <- DBI::dbReadTable(con, 'input_padrao_db')
  # summary(b$similaridade_logradouro)


  # 3rd step: match deterministico --------------------------------------------------------

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

  # 66666666  NAO ESTA
  # Error: Table "output_db" does not have a column with name "similaridade_logradouro"
  # 6666666666


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


  DBI::dbSendQueryArrow(con, query_match)
  # DBI::dbExecute(con, query_aggregate)
  # d <- DBI::dbReadTable(con, 'output_db')
  # d <- DBI::dbReadTable(con, 'aaa')

  # # remove arrow tables from db
  # duckdb::duckdb_unregister_arrow(con, cnefe_table_name) # 6666666
  #
  # #  if (match_type %like% "01") {
  # duckdb::duckdb_unregister_arrow(con, "{aprox_tbl_name}")
  #  }

  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
