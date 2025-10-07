# prolema atual é que a pacote recalcula distancias de string TODA VEZ
# seria melhor criar uma tabela de distancia e ir populando

# 1st step: create small table with unique logradouros
# 2nd step: update input_padrao_db with the most probable logradouro
# 3rd step: deterministic match to update output

match_cases_probabilistic <- function(
    con = con,
    x = 'input_padrao_db',
    y = 'filtered_cnefe',
    output_tb = "output_db",
    key_cols = key_cols,
    match_type = match_type,
    resultado_completo){ # nocov start

  # match_type = "pn01"

  # get corresponding parquet table and key columns
  table_name <- get_reference_table(match_type)
  key_cols <- get_key_cols(match_type)

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

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)



  # 1st step: create small table with unique logradouros -----------------------

  # check if view 'unique_logradouros_cep_localidade' exists
  temp_check <- duckdb::duckdb_list_arrow(conn = con)
  temp_check <- 'unique_logradouros_cep_localidade' %in% temp_check

  if (match_type == 'pn01' | isFALSE(temp_check)) {

    unique_logradouros_cep_localidade <- filtered_cnefe |>
      dplyr::select(dplyr::all_of(c("estado", "municipio", "logradouro", "cep", "localidade"))) |>
      dplyr::distinct() |>
      dplyr::compute()

    # register to db
    duckdb::duckdb_register_arrow(con, "unique_logradouros", unique_logradouros_cep_localidade)
    duckdb::duckdb_register_arrow(con, "unique_logradouros_cep_localidade", unique_logradouros_cep_localidade)
    # a <- DBI::dbReadTable(con, 'unique_logradouros')

  } else {

    # 666 esse passo poderia tmb filtar estados e municipios presentes
    unique_cols <- key_cols[!key_cols %in% "numero"]

    query_unique_logradouros <- glue::glue(
      "CREATE OR REPLACE VIEW unique_logradouros AS
            SELECT DISTINCT {paste(unique_cols, collapse = ', ')}
            FROM unique_logradouros_cep_localidade;"
    )

    DBI::dbSendQueryArrow(con, query_unique_logradouros)
  }


  # 2nd step: update input_padrao_db with the most probable logradouro ---------

  # cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # remove numero and logradouro from key cols to allow for the matching
  key_cols_string_dist <- key_cols[!key_cols %in%  c("numero", "logradouro")]

  join_condition_lookup <- paste(
    glue::glue("unique_logradouros.{key_cols_string_dist} = {x}.{key_cols_string_dist}"),
    collapse = ' AND '
  )

  # min cutoff for string match
  min_cutoff <- get_prob_match_cutoff(match_type)

  # query
  query_lookup <- glue::glue(
    "WITH ranked_data AS (
    SELECT
      {x}.tempidgeocodebr,
      {x}.logradouro AS logradouro,
      unique_logradouros.logradouro AS logradouro_cnefe,
      CAST(jaro_similarity({x}.logradouro, unique_logradouros.logradouro) AS NUMERIC(5,3)) AS similarity,
      RANK() OVER (PARTITION BY {x}.tempidgeocodebr ORDER BY similarity DESC) AS rank
    FROM {x}
    JOIN unique_logradouros
      ON {join_condition_lookup}
    WHERE {cols_not_null} AND {x}.similaridade_logradouro IS NULL AND similarity > {min_cutoff}
  )

  UPDATE {x}
    SET temp_lograd_determ = ranked_data.logradouro_cnefe,
        similaridade_logradouro = similarity
    FROM ranked_data
  WHERE {x}.tempidgeocodebr = ranked_data.tempidgeocodebr
    AND similarity > {min_cutoff}
    AND rank = 1;"
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
      glue::glue("filtered_cnefe.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols, ", input_padrao_db.similaridade_logradouro AS similaridade_logradouro")
  }


  # summarize query
  query_update_db <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, endereco_encontrado, tipo_resultado, desvio_metros, contagem_cnefe {colunas_encontradas})
      SELECT {x}.tempidgeocodebr,
        filtered_cnefe.lat,
        filtered_cnefe.lon,
        filtered_cnefe.endereco_completo AS endereco_encontrado,
        '{match_type}' AS tipo_resultado,
        filtered_cnefe.desvio_metros,
        filtered_cnefe.n_casos AS contagem_cnefe {additional_cols}
      FROM {x}
      LEFT JOIN filtered_cnefe
      ON {join_condition_match}
      WHERE {cols_not_null} AND filtered_cnefe.lon IS NOT NULL;"
  )



  DBI::dbSendQueryArrow(con, query_update_db)
  # DBI::dbExecute(con, query_update_db)
  # c <- DBI::dbReadTable(con, 'output_db')


  # remove arrow tables from db
  duckdb::duckdb_unregister_arrow(con, "unique_logradouros")
  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")



  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
