#' Geocode reverso de coordenadas espaciais no Brasil
#'
#' @description
#' Geocode reverso de coordenadas geográficas para endereços. A função recebe um
#' `sf data frame` com pontos e retorna o endereço mais próximo dando uma
#' distância máxima de busca.
#'
#' @param pontos Uma tabela de dados com classe espacial `sf data frame` no
#'        sistema de coordenadas geográficas SIRGAS 2000, EPSG 4674.
#' @param dist_max Integer. Distancia máxima aceitável (em metros) entre os
#'        pontos de input e o endereço Por padrão, a distância é de 1000 metros.
#' @template verboso
#' @template cache
#' @template n_cores
#'
#' @return Retorna o `sf data.frame` de input adicionado das colunas do endereço
#'         encontrado. O output inclui uma coluna "distancia_metros" que indica
#'         a distância entre o ponto de input e o endereço mais próximo
#'         encontrado.
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' library(geocodebr)
#' library(sf)
#'
#' # ler amostra de dados
#' pontos <- readRDS(
#'     system.file("extdata/pontos.rds", package = "geocodebr")
#'     )
#'
#' pontos <- pontos[1:50,]
#'
#' # geocode reverso
#' df_enderecos <- geocodebr::geocode_reverso(
#'   pontos = pontos,
#'   dist_max = 1000,
#'   verboso = TRUE,
#'   n_cores = 1
#'   )
#'
#' @export
geocode_reverso <- function(pontos,
                            dist_max = 1000,
                            verboso = TRUE,
                            cache = TRUE,
                            n_cores = 1){

  # check input
  checkmate::assert_class(pontos, 'sf')
  checkmate::assert_number(dist_max, lower = 500, upper = 100000) # max 100 Km
  checkmate::assert_logical(verboso)
  checkmate::assert_logical(cache)
  checkmate::assert_number(n_cores)

  # check if geometry type is POINT
  if (any(sf::st_geometry_type(pontos) != 'POINT')) {
    cli::cli_abort("Input precisa ser um sf data frame com geometria do tipo POINT.")
  }

  epsg <- sf::st_crs(pontos)$epsg
  if (epsg != 4674) {
    cli::cli_abort("Dados de input precisam estar com sistema de coordenadas geogr\u00e1ficas SIRGAS 2000, EPSG 4674.")
  }


  # prep input -------------------------------------------------------

  # converte para data.frame
  coords <- sfheaders::sf_to_df(pontos, fill = TRUE)
  data.table::setDT(coords)
  coords[, c('sfg_id', 'point_id') := NULL]
  data.table::setnames(coords, old = c('x', 'y'), new = c('lon', 'lat'))

  # create temp id
  coords[, tempidgeocodebr := 1:nrow(coords) ]

  # convert max_dist to degrees
  # 1 degree of latitude is always 111320 meters
  margin_lat <- dist_max / 111320

  # 1 degree of longitude is 111320 * cos(lat)
  coords[, c("lat_min", "lat_max") := .(lat - margin_lat, lat + margin_lat)]

  coords[, c("lon_min", "lon_max") := .(lon - dist_max / 111320 * cos(lat),
                                        lon + dist_max / 111320 * cos(lat))
  ]

  # get bounding box around input points
  # using a range of max dist around input points
  bbox_lat_min <- min(coords$lat_min)
  bbox_lat_max <- max(coords$lat_max)
  bbox_lon_min <- min(coords$lon_min)
  bbox_lon_max <- max(coords$lon_max)


  # check if input falls within Brazil
  bbox_brazil <- data.frame(
    xmin = -73.99044997,
    ymin = -33.75208127,
    xmax = -28.83594354,
    ymax =   5.27184108
  )

  error_msg <- 'Coordenadas de input localizadas fora do bounding box do Brasil.'
  if(bbox_lon_min < bbox_brazil$xmin |
     bbox_lon_max > bbox_brazil$xmax |
     bbox_lat_min < bbox_brazil$ymin |
     bbox_lat_max > bbox_brazil$ymax) { stop(error_msg) }


  # download cnefe  -------------------------------------------------------

  # downloading cnefe
  cnefe_dir <- geocodebr::download_cnefe(
    tabela = 'municipio_logradouro_numero_cep_localidade',
    verboso = verboso,
    cache = cache
  )


  # limita escopo de busca aos municipios  -------------------------------------------------------

  # determine potential municipalities
  bbox_munis <- readRDS(system.file("extdata/munis_bbox.rds", package = "geocodebr"))

  get_muni <- function(i){ # i=10
    temp <- coords[i,]
    potential_muni <- dplyr::filter(
      bbox_munis,
      xmin <= temp$lon_min &
        xmax >= temp$lon_max &
        ymin <= temp$lat_min &
        ymax >= temp$lat_max)$code_muni
    return(potential_muni)
  }

  potential_munis <- lapply(X=1:nrow(coords), FUN=get_muni)
  potential_munis <- unlist(potential_munis) |> unique()
  potential_munis <- enderecobr::padronizar_municipios(potential_munis)

  # build path to local file
  path_to_parquet <- fs::path(
    listar_pasta_cache(),
    glue::glue("geocodebr_data_release_{data_release}"),
    paste0("municipio_logradouro_numero_cep_localidade.parquet")
  )

  # Load CNEFE data and filter it to include only states
  # present in the input table, reducing the search scope
  # Narrow search global scope of cnefe to bounding box
  filtered_cnefe <- arrow_open_dataset( path_to_parquet ) |>
    dplyr::filter(municipio %in% potential_munis) |>
    dplyr::compute()


  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_table_db", coords,
                       temporary = TRUE)




  # Find cases nearby -------------------------------------------------------

  query_filter_cases_nearby <- glue::glue(
    "SELECT
        input_table_db.*,
        filtered_cnefe.endereco_completo,
        filtered_cnefe.estado,
        filtered_cnefe.municipio,
        filtered_cnefe.logradouro,
        filtered_cnefe.numero,
        filtered_cnefe.cep,
        filtered_cnefe.localidade,
        filtered_cnefe.lat AS lat_cnefe,
        filtered_cnefe.lon AS lon_cnefe
      FROM
        input_table_db, filtered_cnefe
      WHERE
        input_table_db.lat_min < filtered_cnefe.lat
        AND input_table_db.lat_max > filtered_cnefe.lat
        AND input_table_db.lon_min < filtered_cnefe.lon
        AND input_table_db.lon_max > filtered_cnefe.lon;"
  )

  output <- DBI::dbGetQuery(con, query_filter_cases_nearby)


  # organize output -------------------------------------------------

  data.table::setDT(output)
  output[, c('lon_min', 'lon_max', 'lat_min', 'lat_max') := NULL]

  # calculate distances between pairs of coodinates
  # 66666666make it faster with rcpp_distance_haversine
  # output[, distancia_metros2 := rcpp_distance_haversine(
  #             lat, lon, lat_cnefe, lon_cnefe, tolerance = 1e10)]
  output[, distancia_metros := dt_haversine(lat, lon, lat_cnefe, lon_cnefe)]

  # find the closest point
  output <- output[output[, .I[distancia_metros == min(distancia_metros)], by = tempidgeocodebr]$V1]

  # sort data
  output <- output[order(tempidgeocodebr)]
  output[, tempidgeocodebr := NULL]

  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")
  duckdb::dbDisconnect(con)

  # convert df to simple feature
  output_sf <- sfheaders::sf_point(
    obj = output,
    x = 'lon',
    y = 'lat',
    keep = TRUE
  )

  sf::st_crs(output_sf) <- 4674

  return(output_sf)
}

