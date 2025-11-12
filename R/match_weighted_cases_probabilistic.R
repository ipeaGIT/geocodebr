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

  # build path to local file
  path_to_parquet <- fs::path(
    listar_pasta_cache(),
    glue::glue("geocodebr_data_release_{data_release}"),
    paste0(cnefe_table_name,".parquet")
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
  duckdb::duckdb_register_arrow(con, cnefe_table_name, filtered_cnefe)



  # 1st step: create small table with unique logradouros -----------------------

  if (match_type %like% "01") {

    # unique_logradouros_cep_localidade <- filtered_cnefe |>
    #   dplyr::select(dplyr::all_of(c("estado", "municipio", "logradouro", "cep", "localidade"))) |>
    #   dplyr::distinct() |>
    #   dplyr::compute()

    path_unique_cep_loc <- fs::path(
      listar_pasta_cache(),
      glue::glue("geocodebr_data_release_{data_release}"),
      paste0("municipio_logradouro_cep_localidade.parquet")
    )

    unique_logradouros <- arrow_open_dataset( path_unique_cep_loc ) |>
      dplyr::filter(estado %in% input_states) |>
      dplyr::filter(municipio %in% input_municipio) |>
      dplyr::compute()

    # register to db
    duckdb::duckdb_register_arrow(con, "unique_logradouros", unique_logradouros)
    # a <- DBI::dbReadTable(con, 'unique_logradouros')

  } else {

    # 666 esse passo poderia tmb filtar estados e municipios presentes
    unique_cols <- key_cols[!key_cols %in%  "numero"]

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

  join_condition_string_dist <- paste(
    glue::glue("unique_logradouros.{key_cols_string_dist} = {x}.{key_cols_string_dist}"),
    collapse = ' AND '
  )

  # min cutoff for string match
  min_cutoff <- get_prob_match_cutoff(match_type)

  # query update input table with probable logradouro
  query_lookup <- glue::glue(
    "WITH ranked_data AS (
        SELECT
          {x}.tempidgeocodebr,
          unique_logradouros.logradouro AS logradouro_cnefe,
          CAST(jaro_similarity({x}.logradouro, unique_logradouros.logradouro) AS NUMERIC(5,3)) AS similarity,
          RANK() OVER (PARTITION BY {x}.tempidgeocodebr ORDER BY similarity DESC, logradouro_cnefe) AS rank
        FROM {x}
        JOIN unique_logradouros
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
             {y}.logradouro AS logradouro_encontrado,
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

  # remove arrow tables from db
  duckdb::duckdb_unregister_arrow(con, cnefe_table_name) # 6666666

  #  if (match_type %like% "01") {
  duckdb::duckdb_unregister_arrow(con, "unique_logradouros")
  #  }

  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
