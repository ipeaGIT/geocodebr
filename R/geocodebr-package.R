#' Package: geocodebr: Geolocalização De Endereços Brasileiros (Geocoding
#' Brazilian Addresses)
#'
#' @name geocodebr
#' @aliases geocodebr-package
#'
#' @importFrom dplyr mutate select across case_when all_of
#' @importFrom data.table := .I .SD %chin% .GRP .N %like%
#' @importFrom stats weighted.mean
#'
#' @keywords internal
"_PACKAGE"

utils::globalVariables(
  c(
    "year", "temp_local_file", "lon_min", "lon_max", "lat_min", "lat_max", ".",
    "lon_diff", "lat_diff", "lon", "lat", "estado", "municipio",
    "tempidgeocodebr", "input_padrao", "dist_geocodebr", "empate", "contagem_cnefe",
    "temp_lograd_determ", "similaridade_logradouro",

    # due to reverse geocoding draft
    "cep", "lat_cnefe", "localidade", "lon_cnefe", "distancia_metros", "logradouro",
    "numero", "xmax", "xmin", "ymax", "ymin",

    # due to empates
    "endereco_encontrado", "logradouro_encontrado"
  )
)



