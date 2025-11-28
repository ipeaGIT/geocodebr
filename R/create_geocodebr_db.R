create_geocodebr_db <- function( # nocov start
    db_path = "tempdir",
    n_cores = NULL){

  # check input
  checkmate::assert_number(n_cores, null.ok = TRUE)
  # checkmate::assert_string(db_path, pattern = "tempdir|memory")


  # this creates a local database which allows DuckDB to
  # perform **larger-than-memory** workloads
  if(db_path == 'tempdir'){
    db_path <- tempfile(pattern = 'geocodebr', fileext = '.duckdb')
  }

  if(db_path == 'memory'){
    con <- duckdb::dbConnect(duckdb::duckdb(), dbdir= ":memory:" )
  }

  # create db connection
  con <- duckdb::dbConnect(
    duckdb::duckdb( bigint = "integer64" ),
    dbdir= db_path
    ) # db_path ":memory:"

  # Set Number of cores for parallel operation
  if (is.null(n_cores)) {
    n_cores <- min(
      parallelly::availableCores(),
      parallelly::freeConnections()
    )
  }

  # Set threads
  DBI::dbExecute(con, sprintf("SET threads = %s;", n_cores))


  # Silence progress bar from duckdb
  DBI::dbExecute(con, "SET enable_progress_bar = false")



  # Set Memory limit
  # DBI::dbExecute(con, "SET memory_limit = '8GB'")

  # DBI::dbExecute(con, "INSTALL arrow FROM community; LOAD arrow;")
  # DBI::dbExecute(con, "LOAD arrow;")

  return(con)
} # nocov end
