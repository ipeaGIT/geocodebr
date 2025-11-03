match_cases <- function( # nocov start
  con = con,
  x = "input_padrao_db",
  y = "filtered_cnefe",
  output_tb = "output_db",
  key_cols = key_cols,
  match_type = match_type,
  resultado_completo){

  # match_type = "dn01"

  # read corresponding parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)

  # get corresponding parquet table
  table_name <- get_reference_table(match_type)

  # build path to local file
  path_to_parquet <- fs::path(
    listar_pasta_cache(),
    glue::glue("geocodebr_data_release_{data_release}"),
    paste0(table_name,".parquet")
  )

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

  # c <- collect(filtered_cnefe)
  # summary(c$desvio_metros)

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)

  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("filtered_cnefe.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  # cols from x that cannot be null
  # isso serve como filtro pre-join, pra fazer o join soh em quem nao foi encontrado ainda
  cols_not_null <-  paste(
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
      glue::glue("filtered_cnefe.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols)

  }

  # summarize query
  query_match <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, endereco_encontrado, tipo_resultado, desvio_metros, log_causa_confusao, contagem_cnefe {colunas_encontradas})
      SELECT {x}.tempidgeocodebr,
        filtered_cnefe.lat,
        filtered_cnefe.lon,
        filtered_cnefe.endereco_completo AS endereco_encontrado,
        '{match_type}' AS tipo_resultado,
        filtered_cnefe.desvio_metros,
        {x}.log_causa_confusao,
        filtered_cnefe.n_casos AS contagem_cnefe {additional_cols}
      FROM {x}
      LEFT JOIN filtered_cnefe
      ON {join_condition}
      WHERE {cols_not_null} AND filtered_cnefe.lon IS NOT NULL;"
  )

  DBI::dbSendQueryArrow(con, query_match)
  # a <- DBI::dbReadTable(con, 'output_db')
  # summary(a$desvio_metros)
  # summary(a$lat)

  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
