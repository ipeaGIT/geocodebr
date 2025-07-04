#' Safely use arrow to open a Parquet file
#'
#' This function handles some failure modes, including if the Parquet file is
#' corrupted.
#'
#' @param filename A local Parquet file
#' @return An `arrow::Dataset`
#'
#' @keywords internal
arrow_open_dataset <- function(filename){

  tryCatch(
    arrow::open_dataset(filename),
    error = function(e){
      msg <- paste(
        "Arquivo local possivelmente corrompido. ",
        "Apague os arquivos do cache com 'geocodebr::deletar_pasta_cache()' e tente novamente.",
        sep = "\n"
      )
      stop(msg)
    }
  )
}

#' Message when caching file
#'
#' @param local_file The address of a file passed from the download_file function.
#' @param cache Logical. Whether the cached data should be used.

#' @return A message
#'
#' @keywords internal
cache_message <- function(local_file = parent.frame()$local_file,
                          cache = parent.frame()$cache){

  # name of local file
  file_name <- basename(local_file[1])
  dir_name <- dirname(local_file[1])

  ## if file already exists
    # YES cache
    if (file.exists(local_file) & isTRUE(cache)) {
       message('Reading data cached locally.')
       }

    # NO cache
    if (file.exists(local_file) & isFALSE(cache)) {
       message('Overwriting data cached locally.')
       }

  ## if file does not exist yet
  # YES cache
  if (!file.exists(local_file) & isTRUE(cache)) {
     message(paste("Downloading data and storing it locally for future use."))
     }

  # NO cache
  if (!file.exists(local_file) & isFALSE(cache)) {
     message(paste("Downloading data. Setting 'cache = TRUE' is strongly recommended to speed up future use. File will be stored locally at:", dir_name))
     }
  }


#' Update input_padrao_db to remove observations previously matched
#'
#' @param con A db connection
#' @param update_tb String. Name of a table to be updated in con
#' @param reference_tb A table written in con used as reference
#'
#' @return Drops observations from input_padrao_db
#'
#' @keywords internal
update_input_db <- function(con, update_tb = 'input_padrao_db', reference_tb){

  # update_tb = 'input_padrao_db'
  # reference_tb = 'output_caso_1'

  query_remove_matched <- glue::glue("
    DELETE FROM {update_tb}
    WHERE tempidgeocodebr IN (SELECT tempidgeocodebr FROM {reference_tb});")

  DBI::dbExecute(con, query_remove_matched)
}


#' Add a column with info of geocode match_type
#'
#' @param con A db connection
#' @param update_tb String. Name of a table to be updated in con
#'
#' @return Adds a new column to a table in con
#'
#' @keywords internal
add_precision_col <- function(con, update_tb = NULL){

  # update_tb = "output_db"

  # add empty column
  query_add_col <- glue::glue("ALTER TABLE {update_tb} ADD COLUMN precisao TEXT;")
  DBI::dbSendQueryArrow(con, query_add_col)

  # populate column
  query_precision_cats <- glue::glue("
  UPDATE {update_tb}
  SET precisao = CASE
  WHEN tipo_resultado IN ('dn01', 'dn02', 'dn03', 'dn04',
                          'pn01', 'pn02', 'pn03', 'pn04') THEN 'numero'
  WHEN tipo_resultado IN ('da01', 'da02', 'da03', 'da04',
                          'pa01', 'pa02', 'pa03', 'pa04') THEN 'numero_aproximado'
  WHEN tipo_resultado IN ('dl01', 'dl02', 'dl03', 'dl04',
                          'pl01', 'pl02', 'pl03', 'pl04') THEN 'logradouro'
  WHEN tipo_resultado IN ('dc01', 'dc02') THEN 'cep'
  WHEN tipo_resultado = 'db01' THEN 'localidade'
  WHEN tipo_resultado = 'dm01' THEN 'municipio'
  ELSE NULL
  END;")


  # DBI::dbExecute(con, query_precision_cats )
  DBI::dbSendQueryArrow(con, query_precision_cats )
}





merge_results <- function(con,
                          x,
                          y,
                          key_column,
                          select_columns,
                          resultado_completo){


  select_columns_y <- c('lat', 'lon', 'tipo_resultado', 'precisao',
                        'endereco_encontrado', 'logradouro_encontrado', 'contagem_cnefe')

  if (isTRUE(resultado_completo)) {

    # select additional columns to output
    select_columns_y <- c(select_columns_y, 'numero_encontrado' , 'cep_encontrado',
                          'localidade_encontrada', 'municipio_encontrado' ,
                          'estado_encontrado', 'similaridade_logradouro')

    # relace NULL similaridade_logradouro as 1 because they were found deterministically
    DBI::dbSendQueryArrow(
      con,
      "UPDATE output_db
      SET similaridade_logradouro = COALESCE(similaridade_logradouro, 1);"
    )

  }

  # Create the SELECT clause dynamically
  select_x <- paste0(x, '.', c(select_columns), collapse = ', ')

  select_clause <- paste0(
    select_x, ',',
    paste0('sorted_output', ".", select_columns_y, collapse = ", ")
    )

  # Create the JOIN clause dynamically
  join_condition <- paste(
    glue::glue("{x}.{key_column} = sorted_output.{key_column}"),
    collapse = ' ON '
  )

  # Create SQL query
  query <- glue::glue(
    "SELECT {select_clause}
      FROM {x}
      LEFT JOIN (
    SELECT * FROM {y}
      ORDER BY tempidgeocodebr ) AS sorted_output
      ON {join_condition};"
    )

  # Execute the query and fetch the merged data
  merged_data <- DBI::dbGetQuery(con, query)

  return(merged_data)
  }




#' create index
#'
#' @keywords internal
create_index <- function(con, tb, cols, operation, overwrite=TRUE){

  idx <- paste0('idx_', tb)
  cols_group <- paste(cols, collapse = ", ")

  # check if table already has index
  i <- DBI::dbGetQuery(
    con,
    sprintf("SELECT * FROM duckdb_indexes WHERE table_name = '%s';", tb)
  )

  if (nrow(i) > 0 & isFALSE(overwrite)) { return(NULL) }
  if (nrow(i) > 0 & isTRUE(overwrite)) {
    DBI::dbExecute(con, sprintf('DROP INDEX IF EXISTS %s',idx))
  }

  query_index <- sprintf('%s INDEX %s ON %s(%s);', operation, idx, tb, cols_group)
  DBI::dbExecute(con, query_index)
}




get_key_cols <- function(match_type) {
  relevant_cols <- if (match_type %in% c('dn01', 'da01', 'pn01', 'pa01') ) {
    c("estado", "municipio", "logradouro", "numero", "cep", "localidade")
  } else if (match_type %in% c('dn02', 'da02', 'pn02', 'pa02')) {
    c("estado", "municipio", "logradouro", "numero", "cep")
  } else if (match_type %in% c('dn03', 'da03', 'pn03', 'pa03')) {
    c("estado", "municipio", "logradouro", "numero", "localidade")
  } else if (match_type %in% c('dn04', 'da04', 'pn04', 'pa04')) {
    c("estado", "municipio", "logradouro", "numero")
  } else if (match_type %in% c('dl01', 'pl01')) {
    c("estado", "municipio", "logradouro", "cep", "localidade")
  } else if (match_type %in% c('dl02', 'pl02')) {
    c("estado", "municipio", "logradouro", "cep")
  } else if (match_type %in% c('dl03', 'pl03')) {
    c("estado", "municipio", "logradouro", "localidade")
  } else if (match_type %in% c('dl04', 'pl04')) {
    c("estado", "municipio", "logradouro")
  } else if (match_type == 'dc01') {
    c("estado", "municipio", "cep", "localidade")
  } else if (match_type == 'dc02') {
    c("estado", "municipio", "cep")
  } else if (match_type == 'db01') {
    c("estado", "municipio", "localidade")
  } else if (match_type == 'dm01') {
    c("estado", "municipio")
  }

  return(relevant_cols)
}

### ideal sequence of match types
all_possible_match_types <- c(
  "dn01", "da01", "pn01", "pa01",
  "dn02", "da02", "pn02", "pa02",
  "dn03", "da03", "pn03", "pa03",
  "dn04", "da04", #"pn04", "pa04", # too costly
  "dl01",         "pl01",
  "dl02",         "pl02",
  "dl03",         "pl03",
  "dl04",         # pl04",  # too costly
  "dc01", "dc02", "db01", "dm01"
)

# ### 2nd best viable sequence of match types for really large datasets ? testando com cadunico
# all_possible_match_types <- c(
#   "dn01", "da01",
#   "dn02", "da02",
#   "dn03", "da03",
#   "dn04", "da04",
#   "pn01", "pa01", "pn02", "pa02", "pn03", "pa03", #"pn04", "pa04", # too costly
#   "dl01",         "pl01",
#   "dl02",         "pl02",
#   "dl03",         "pl03",
#   "dl04",         # pl04",  # too costly
#   "dc01", "dc02", "db01", "dm01"
# )






number_exact_types <- c(
  "dn01", "dn02", "dn03", "dn04"
  )

number_interpolation_types <- c(
  "da01", "da02", "da03", "da04"
  )

probabilistic_exact_types <- c(
  "pn01", "pn02", "pn03", "pn04"

  )

probabilistic_interpolation_types <- c(
  "pa01", "pa02", "pa03", "pa04"
  )

exact_types_no_number <- c(
  "dl01", "dl02", "dl03", "dl04",
  "dc01", "dc02", "db01", "dm01"
  )

probabilistic_types_no_number <- c(
  "pl01", "pl02", "pl03", "pl04"
  )

exact_types__no_logradouro <- c(
  "dc01", "dc02", "db01", "dm01"
)







assert_and_assign_address_fields <- function(address_fields, addresses_table) {
  possible_fields <- c(
    "logradouro", "numero", "cep", "localidade", "municipio", "estado"
  )

  col <- checkmate::makeAssertCollection()
  checkmate::assert_names(
    names(address_fields),
    type = "unique",
    subset.of = possible_fields,
    add = col
  )
  checkmate::assert_names(
    address_fields,
    subset.of = names(addresses_table),
    add = col
  )
  checkmate::reportAssertions(col)

  missing_fields <- setdiff(possible_fields, names(address_fields))

  missing_fields_list <- vector(mode = "list", length = length(missing_fields))
  names(missing_fields_list) <- missing_fields

  complete_fields_list <- append(as.list(address_fields), missing_fields_list)

  return(complete_fields_list)
}



get_reference_table <- function(match_type){

  # key_cols = get_key_cols('da03')

  key_cols <- get_key_cols(match_type)

  # read corresponding parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)

  # reference table
  if (match_type %like% 'dn02|pn02|da02|pa02|dn03|pn03') {
    table_name <- "municipio_logradouro_numero_cep_localidade"
  }

  if (match_type %like% 'da03|pa03|dn04|da04') {
    table_name <- "municipio_logradouro_numero_localidade"
  }

  if (match_type %like% 'dl02|pl02|dl03|pl03') {
    table_name <- "municipio_logradouro_cep_localidade"
  }

  if (match_type %like% 'dl04') {
    table_name <- "municipio_logradouro_localidade"
  }

  return(table_name)
  }


# min cutoff for string match
# min cutoff for probabilistic string match of logradouros
get_prob_match_cutoff <- function(match_type){
  min_cutoff <- ifelse(match_type %in% c('pn01', 'pa01', 'pl01'), 0.85,  0.9)
  return(min_cutoff)
  }



# create a dummy function that uses nanoarrow with no effect
# nanoarrow is only used internally in DBI::dbWriteTableArrow()
# however, if we do not put this dummy function here, CRAN check flags an error
dummy <- function() {
  nanoarrow::as_nanoarrow_schema
  }
