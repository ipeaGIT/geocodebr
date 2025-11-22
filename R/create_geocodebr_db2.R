
# ---------------------------------------------------------------
# ensure_geocodebr_db()
# Creates or reuses a persistent DuckDB database containing
# all cached CNEFE tables materialized and indexed.
# ---------------------------------------------------------------

create_geocodebr_db2 <- function(
    data_release = data_release,               # use pkg default
    n_cores = NULL){


  cache_dir <- geocodebr::listar_pasta_cache()

  # Determine DB path
  db_path <- file.path(
    cache_dir,
    paste0("geocodebr_",
           data_release,
           ".duckdb")
    )

  # create db connection
  con <- duckdb::dbConnect(
    duckdb::duckdb( bigint = "integer64" ),
    dbdir = db_path,
    read_only = FALSE
  )
  DBI::dbExecute(con, sprintf("SET threads = %s;", n_cores))



  # If already connected and consistent, return existing conn
  if (file.exists(db_path) && DBI::dbIsValid(con)) {
    return(con)
  }

  # Check if CNEFE tables already exist
  existing <- DBI::dbGetQuery(con, "SELECT table_name FROM information_schema.tables")

  required_tables <- c(
    "municipio",
    "municipio_cep",
    "municipio_cep_localidade",
    "municipio_localidade"
  )

  needs_build <- !all(required_tables %in% existing$table_name)

  if (needs_build) {
    message("geocodebr: initializing persistent DB… This is done only once per data release.")
    materialize_cached_cnefe_to_duckdb(con, required_tables)
  }

  return(con)
}


# ---------------------------------------------------------------------
# Materialize all cached Parquet CNEFE tables into DuckDB once per release
# ---------------------------------------------------------------------

materialize_cached_cnefe_to_duckdb <- function(con, required_tables) {

  # list cache contents (existing package function)
  cache_files <- geocodebr::listar_dados_cache()

  # keep only parquet files
  parquet_files <- cache_files[grepl("\\.parquet", cache_files)]

  parquet_files <- lapply(
    required_tables,
    FUN = function(f){cache_files[grepl(paste0(f,".parquet"), cache_files)]}
    ) |>
    unlist()


  for (i in 1:length(parquet_files)) {

    parquet_path <- parquet_files[i]
    tbl_name <- fs::path_ext_remove(basename(parquet_path))

    message("   Loading ", tbl_name, " …")

    sql <- glue::glue("
      CREATE OR REPLACE TABLE {tbl_name} AS
      SELECT * FROM read_parquet('{parquet_path}');
    ")

    DBI::dbExecute(con, sql)

    # Indexing strategy: use the existing create_index() function
      try({
        create_index(
          con = con,
          tbl_name = tbl_name,
          index_type = "BTREE"
        )
      }, silent = TRUE)

  }

  invisible(TRUE)
}



create_index <- function(
    con,
    tbl_name,
    index_type = "BTREE" # BTREE RTREE
) {

#
#   if (missing(con) || is.null(con)) {
#     stop("create_index(): argument 'con' must be a valid DuckDB connection.")
#   }
#   if (missing(table_name) || !nzchar(table_name)) {
#     stop("create_index(): 'table_name' must be provided.")
#   }
#   if (missing(cols) || length(cols) == 0) {
#     stop("create_index(): argument 'cols' must be a non-empty character vector.")
#   }


  cols <- strsplit(x = tbl_name, "_") |> unlist()
  # index name
  idx_name <- paste0(
    "idx_",
    tbl_name
    )

  # build column expression
  cols_expr <- paste(cols, collapse = ", ")


  if (index_type == "BTREE") {

    sql <- glue::glue("
      CREATE INDEX IF NOT EXISTS {idx_name}
      ON {tbl_name} ({cols_expr});
    ")

  } else if (index_type == "RTREE") {

    # RTREE index requires DuckDB spatial extension
    sql <- glue::glue("
      CREATE INDEX IF NOT EXISTS {idx_name}
      ON {tbl_name}
      USING RTREE ({cols_expr});
    ")

  }

  tryCatch(
    DBI::dbExecute(con, sql),
    error = function(e) {
      message("create_index(): failed to create index ", idx_name,
              " on table ", tbl_name, " - ", e$message)
    }
  )

  return(invisible(TRUE))


}
