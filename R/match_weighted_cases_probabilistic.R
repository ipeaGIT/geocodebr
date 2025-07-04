# 1st step: create small table with unique logradouros
# 2nd step: update input_padrao_db with the most probable logradouro
# 3rd step: deterministic match
# 4th step: aggregate

match_weighted_cases_probabilistic <- function( # nocov start
  con = con,
  x = 'input_padrao_db',
  y = 'filtered_cnefe',
  output_tb = "output_db",
  key_cols = key_cols,
  match_type = match_type,
  resultado_completo){

  # get corresponding parquet table
  table_name <- get_reference_table(match_type)
  key_cols <- get_key_cols(match_type)

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



  # 1st step: create small table with unique logradouros -----------------------

  # get_reference_table('pa02')
  # get_key_cols('pa02')
  # if (match_type %like% "01") {
  #
  #   path_unique_cep_loc <- paste0(listar_pasta_cache(), "/municipio_logradouro_cep_localidade.parquet")
  #
  #   unique_logradouros <- arrow_open_dataset( path_unique_cep_loc ) |>
  #     dplyr::filter(estado %in% input_states) |>
  #     dplyr::filter(municipio %in% input_municipio) |>
  #     dplyr::compute()
  #
  #   } else {
  #
  #     unique_cols <- key_cols[!key_cols %in%  "numero"]
  #
  #     unique_logradouros <- filtered_cnefe |>
  #       dplyr::select( dplyr::all_of(unique_cols)) |>
  #       dplyr::distinct() |>
  #       dplyr::compute()
  #
  # }
  # # register to db
  # duckdb::duckdb_register_arrow(con, "unique_logradouros", unique_logradouros)
  # # a <- DBI::dbReadTable(con, 'unique_logradouros')

  if (match_type %like% "01") {

    # unique_logradouros_cep_localidade <- filtered_cnefe |>
    #   dplyr::select(dplyr::all_of(c("estado", "municipio", "logradouro", "cep", "localidade"))) |>
    #   dplyr::distinct() |>
    #   dplyr::compute()

    path_unique_cep_loc <- paste0(listar_pasta_cache(), "/municipio_logradouro_cep_localidade.parquet")
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
      {x}.logradouro AS logradouro,
      unique_logradouros.logradouro AS logradouro_cnefe,
      CAST(jaro_similarity({x}.logradouro, unique_logradouros.logradouro) AS NUMERIC(5,3)) AS similarity,
      RANK() OVER ( PARTITION BY {x}.tempidgeocodebr ORDER BY similarity DESC ) AS rank
    FROM {x}
    JOIN unique_logradouros
      ON {join_condition_string_dist}
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

  # match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
    SELECT {x}.tempidgeocodebr, {x}.numero, {y}.numero AS numero_cnefe,
    {y}.lat, {y}.lon,
    REGEXP_REPLACE( {y}.endereco_completo, ', \\d+ -', CONCAT(', ', {x}.numero, ' (aprox) -')) AS endereco_encontrado,
    {x}.similaridade_logradouro,
    {y}.logradouro AS logradouro_encontrado,
    {y}.n_casos AS contagem_cnefe {additional_cols}
    FROM {x}
    LEFT JOIN {y}
    ON {join_condition_determ}
    WHERE lon IS NOT NULL {cols_not_null_match};"
  )

  DBI::dbSendQueryArrow(con, query_match)
  # DBI::dbExecute(con, query_match)
  # c <- DBI::dbReadTable(con, 'temp_db')



  # 4th step: aggregate --------------------------------------------------------

  # summarize query
  # 66666666666 passar para esse passo a construcao do endereco_encontrado
  query_aggregate <- glue::glue(
    # "CREATE OR REPLACE TEMPORARY TABLE aaa AS
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado,
                            endereco_encontrado, contagem_cnefe)
      SELECT tempidgeocodebr,
      SUM((1/ABS(numero - numero_cnefe) * lat)) / SUM(1/ABS(numero - numero_cnefe)) AS lat,
      SUM((1/ABS(numero - numero_cnefe) * lon)) / SUM(1/ABS(numero - numero_cnefe)) AS lon,
      '{match_type}' AS tipo_resultado,
      FIRST(endereco_encontrado) AS endereco_encontrado,
      FIRST(contagem_cnefe) AS contagem_cnefe
      FROM temp_db
      GROUP BY tempidgeocodebr, endereco_encontrado;"
  )



  if (isTRUE(resultado_completo)) {

    key_cols <- get_key_cols(match_type)
    key_cols <- key_cols[key_cols != 'numero']

    additional_cols <- paste0(
      glue::glue("FIRST({key_cols}_encontrado) AS {key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols)

    query_aggregate <- glue::glue(
      # "CREATE OR REPLACE TEMPORARY TABLE aaa AS
      "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado,
                              endereco_encontrado, similaridade_logradouro,
                              contagem_cnefe {colunas_encontradas})
       SELECT tempidgeocodebr,
       SUM((1/ABS(numero - numero_cnefe) * lat)) / SUM(1/ABS(numero - numero_cnefe)) AS lat,
        SUM((1/ABS(numero - numero_cnefe) * lon)) / SUM(1/ABS(numero - numero_cnefe)) AS lon,
        '{match_type}' AS tipo_resultado,
        FIRST(endereco_encontrado) AS endereco_encontrado,
        FIRST(similaridade_logradouro) AS similaridade_logradouro,
        FIRST(contagem_cnefe) AS contagem_cnefe
        {additional_cols}
      FROM temp_db
      GROUP BY tempidgeocodebr, endereco_encontrado;"
    )
    # #  "CREATE OR REPLACE TEMPORARY TABLE aaa AS
    # "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado,
    #                       endereco_encontrado, similaridade_logradouro,
    #                       contagem_cnefe {colunas_encontradas})
    #  SELECT tempidgeocodebr,


  }

  DBI::dbSendQueryArrow(con, query_aggregate)
  # DBI::dbExecute(con, query_aggregate)
  # d <- DBI::dbReadTable(con, 'output_db')
  # d <- DBI::dbReadTable(con, 'aaa')

  # remove arrow tables from db
  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

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



# # fazer esse teste aqui com cats Prob Aprox
# # especiamente o passo de criar passo 1.
#
#
# library(dplyr)
# dfgeo <- readRDS("dfgeo2.rds")
#
#
# match_type <- 'pa03'
# idsss <- filter(dfgeo, tipo_resultado %in% match_type)$id
#
# input_padrao_temp <- filter(input_padrao, tempidgeocodebr %in% idsss)
#
# # Convert input data frame to DuckDB table
# duckdb::dbWriteTable(con, "input_padrao_db", input_padrao_temp,
#                      overwrite = TRUE, temporary = TRUE)
#
#
# key_cols <- get_key_cols(match_type)
#
# bench::mark(
#   match_weighted_cases_probabilistic(  # match_cases_probabilistic_old
#     con = con,
#     x = 'input_padrao_db',
#     y = 'filtered_cnefe',
#     output_tb = "output_db",
#     key_cols = key_cols,
#     match_type = match_type,
#     resultado_completo = TRUE)
# )
#
# c <- DBI::dbReadTable(con, 'output_db')
#
# # expression    min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
# # atual1      122ms  123ms      8.14    16.9MB     8.14     2     2      246ms <dbl>  <Rprofmem> <bench_tm>
# # atual2      116ms  121ms      8.31     459KB     2.77     3     1      361ms <dbl>  <Rprofmem> <bench_tm>
# # atual3      114ms  122ms      8.19     459KB     2.05     4     1      489ms <dbl>  <Rprofmem> <bench_tm>
#
# # new1        108ms  112ms      8.93    14.1MB     8.93     2     2      224ms <dbl>  <Rprofmem> <bench_tm>
# # new2        143ms  147ms      6.52    3.32MB     2.17     3     1      460ms <dbl>  <Rprofmem> <bench_tm>
# # new3        151ms  156ms      6.39     613KB     3.20     2     1      313ms <dbl>  <Rprofmem> <bench_tm>
