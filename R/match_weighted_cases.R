match_weighted_cases <- function( # nocov start
  con = con,
  x = 'input_padrao_db',
  y = 'filtered_cnefe',
  output_tb = "output_db",
  key_cols = key_cols,
  match_type = match_type,
  resultado_completo){

  # match_type = "da01"

  # get corresponding parquet table
  table_name <- get_reference_table(match_type)

  # build path to local file
  path_to_parquet <- paste0(listar_pasta_cache(), "/", table_name, ".parquet")

  # determine geographical scope of the search
  input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado
  input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio

  # Load CNEFE data and write to DuckDB
  # filter cnefe to include only states and municipalities
  # present in the input table, reducing the search scope
  filtered_cnefe <- arrow_open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)

  # cols that cannot be null
  cols_not_null <-  paste(
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
  additional_cols <- ""

  if (isTRUE(resultado_completo)) {

    colunas_encontradas <- paste0(
      glue::glue("{key_cols}_encontrado"),
      collapse = ', ')

    colunas_encontradas <- gsub('localidade_encontrado', 'localidade_encontrada', colunas_encontradas)
    colunas_encontradas <- paste0(", ", colunas_encontradas)

    additional_cols <- paste0(
      glue::glue("filtered_cnefe.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols)

  }


  # 1st step: match  --------------------------------------------------------

  # match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
      SELECT {x}.tempidgeocodebr, {x}.numero,
        {y}.numero AS numero_cnefe,
        {y}.lat, {y}.lon,
        REGEXP_REPLACE( {y}.endereco_completo, ', \\d+ -', CONCAT(', ', {x}.numero, ' (aprox) -')) AS endereco_encontrado,
        {y}.desvio_metros,
        {y}.n_casos AS contagem_cnefe {additional_cols}
    FROM {x}
    LEFT JOIN {y}
    ON {join_condition}
    WHERE {cols_not_null} AND {y}.numero IS NOT NULL AND lon IS NOT NULL;"
  )

  DBI::dbSendQueryArrow(con, query_match)
  # a <- DBI::dbReadTable(con, 'temp_db')
  # summary(a$desvio_metros)


  # 2nd step: aggregate --------------------------------------------------------

  # summarize query
  query_aggregate <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, endereco_encontrado, tipo_resultado, desvio_metros, contagem_cnefe)
      SELECT tempidgeocodebr,
        SUM((1/ABS(numero - numero_cnefe) * lat)) / SUM(1/ABS(numero - numero_cnefe)) AS lat,
        SUM((1/ABS(numero - numero_cnefe) * lon)) / SUM(1/ABS(numero - numero_cnefe)) AS lon,
        FIRST(endereco_encontrado) AS endereco_encontrado,
        '{match_type}' AS tipo_resultado,
        AVG(desvio_metros) as desvio_metros,
        FIRST(contagem_cnefe) AS contagem_cnefe
    FROM temp_db
    GROUP BY tempidgeocodebr, endereco_encontrado;"
  )

  # b <- DBI::dbGetQuery(con, query_aggregate)


  if (isTRUE(resultado_completo)) {

    additional_cols <- paste0(
      glue::glue("FIRST({key_cols}_encontrado)"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols)

    query_aggregate <- glue::glue(
      "INSERT INTO output_db (tempidgeocodebr, lat, lon, endereco_encontrado, tipo_resultado, desvio_metros, contagem_cnefe {colunas_encontradas})
        SELECT tempidgeocodebr,
          SUM((1/ABS(numero - numero_cnefe) * lat)) / SUM(1/ABS(numero - numero_cnefe)) AS lat,
          SUM((1/ABS(numero - numero_cnefe) * lon)) / SUM(1/ABS(numero - numero_cnefe)) AS lon,
          FIRST(endereco_encontrado) AS endereco_encontrado,
          '{match_type}' AS tipo_resultado,
          MAX(desvio_metros) as desvio_metros,
          FIRST(contagem_cnefe) AS contagem_cnefe {additional_cols}
      FROM temp_db
      GROUP BY tempidgeocodebr, endereco_encontrado;"
    )
  }

  DBI::dbSendQueryArrow(con, query_aggregate)
  # b <- DBI::dbReadTable(con, 'output_db')
  # summary(b$desvio_metros)

  # b <- DBI::dbGetQuery(con, query_aggregate)


  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
