#' Geolocaliza endereços no Brasil
#'
#' Geocodifica endereços brasileiros com base nos dados do CNEFE. Os endereços
#' de input devem ser passados como um `data.frame`, no qual cada coluna
#' descreve um campo do endereço (logradouro, número, cep, etc). Os resuldos dos
#' endereços geolocalizados podem seguir diferentes níveis de precisão. Consulte
#' abaixo a seção "Detalhes" para mais informações. As coordenadas de output
#' utilizam o sistema de coordenadas geográficas SIRGAS 2000, EPSG 4674.
#'
#' @param enderecos Um `data.frame`. Os endereços a serem geolocalizados. Cada
#'    coluna deve representar um campo do endereço.
#' @param campos_endereco Um vetor de caracteres. A correspondência entre cada
#'    campo de endereço e o nome da coluna que o descreve na tabela `enderecos`.
#'    A função [definir_campos()] auxilia na criação deste vetor e realiza
#'    algumas verificações nos dados de entrada. Campos de endereço passados
#'    como `NULL` serão ignorados, e a função deve receber pelo menos um campo
#'    não nulo, além  dos campos `"estado"` e `"municipio"`, que são
#'    obrigatórios. Note que o campo  `"localidade"` é equivalente a 'bairro'.
#' @param resultado_completo Lógico. Indica se o output deve incluir colunas
#'    adicionais, como o endereço encontrado de referência. Por padrão, é `FALSE`.
#' @param resolver_empates Lógico. Alguns resultados da geolocalização podem
#'    indicar diferentes coordenadas possíveis (e.g. duas ruas diferentes com o
#'    mesmo nome em uma mesma cidade). Esses casos são trados como 'empate' e o
#'    parâmetro `resolver_empates` indica se a função deve resolver esses empates
#'    automaticamente. Por padrão, é `FALSE`, e a função retorna apenas o caso
#'    mais provável. Para mais detalhes sobre como é feito o processo de
#'    desempate, consulte abaixo a seção "Detalhes".
#' @template h3_res
#' @template resultado_sf
#' @template verboso
#' @template cache
#' @template n_cores
#'
#' @return Retorna o `data.frame` de input `enderecos` adicionado das colunas de
#'   latitude (`lat`) e longitude (`lon`), bem como as colunas (`precisao` e
#'   `tipo_resultado`) que indicam o nível de precisão e o tipo de resultado.
#'   Alternativamente, o resultado pode ser um objeto `sf`.
#'
#' @template precision_section
#' @template empates_section
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' library(geocodebr)
#'
#' # ler amostra de dados
#' data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
#' input_df <- read.csv(data_path)[1:2,]
#'
#' fields <- geocodebr::definir_campos(
#'   logradouro = "nm_logradouro",
#'   numero = "Numero",
#'   cep = "Cep",
#'   localidade = "Bairro",
#'   municipio = "nm_municipio",
#'   estado = "nm_uf"
#' )
#'
#' df <- geocodebr::geocode(
#'   enderecos = input_df,
#'   campos_endereco = fields,
#'   resolver_empates = TRUE,
#'   verboso = FALSE
#'   )
#'
#' head(df)
#'
#' @export
geocode <- function(enderecos,
                    campos_endereco = definir_campos(),
                    resultado_completo = FALSE,
                    resolver_empates = FALSE,
                    h3_res = NULL,
                    resultado_sf = FALSE,
                    verboso = TRUE,
                    cache = TRUE,
                    n_cores = 1 ){

  # check input
  checkmate::assert_data_frame(enderecos)
  checkmate::assert_logical(resultado_completo, any.missing = FALSE, len = 1)
  checkmate::assert_logical(resolver_empates, any.missing = FALSE, len = 1)
  checkmate::assert_logical(resultado_sf, any.missing = FALSE, len = 1)
  checkmate::assert_logical(verboso, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)
  checkmate::assert_number(h3_res, null.ok = TRUE, lower = 0, upper = 15)
  campos_endereco <- assert_and_assign_address_fields(
    campos_endereco,
    enderecos
  )

  # normalize input data -------------------------------------------------------

  # standardizing the addresses table to increase the chances of finding a match
  # in the CNEFE data

  if (verboso) message_standardizing_addresses()

  input_padrao <- enderecobr::padronizar_enderecos(
    enderecos,
    campos_do_endereco = enderecobr::correspondencia_campos(
      logradouro = campos_endereco[["logradouro"]],
      numero = campos_endereco[["numero"]],
      cep = campos_endereco[["cep"]],
      bairro = campos_endereco[["localidade"]],
      municipio = campos_endereco[["municipio"]],
      estado = campos_endereco[["estado"]]
    ),
    formato_estados = "sigla",
    formato_numeros = 'integer'
  )


  # keep and rename colunms of input_padrao to use the
  # same column names used in cnefe data set
  data.table::setDT(input_padrao)
  cols_to_keep <- names(input_padrao)[ names(input_padrao) %like% '_padr']
  input_padrao <- input_padrao[, .SD, .SDcols = c(cols_to_keep)]
  names(input_padrao) <- c(gsub("_padr", "", names(input_padrao)))

  if ('bairro' %in% names(input_padrao)) {
    data.table::setnames(
      x = input_padrao, old = 'bairro', new = 'localidade'
    )
  }

  # create temp id
  data.table::setDT(enderecos)[, tempidgeocodebr := 1:nrow(input_padrao) ]
  input_padrao[, tempidgeocodebr := 1:nrow(input_padrao) ]

  # temp coluna de logradouro q sera usada no match probabilistico
  input_padrao[, temp_lograd_determ := NA_character_ ]
  input_padrao[, similaridade_logradouro := NA_real_ ]

  # # sort input data
  # input_padrao <- input_padrao[order(estado, municipio, logradouro, numero, cep, localidade)]

  # downloading cnefe
  cnefe_dir <- download_cnefe(
    tabela = 'todas',
    verboso = verboso,
    cache = cache
  )

  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # register standardized input data
  input_padrao_arrw <- arrow::as_arrow_table(input_padrao)
  DBI::dbWriteTableArrow(con, name = "input_padrao_db", input_padrao_arrw,
                         overwrite = TRUE, temporary = TRUE)



  # cria coluna "log_causa_confusao" identificando logradouros que geram confusao
  # issue https://github.com/ipeaGIT/geocodebr/issues/67
  cria_col_logradouro_confusao(con)

    # b <-   DBI::dbReadTable(con, name = 'input_padrao_db') |>
    #     filter(log_causa_confusao==0)


  # create an empty output table that will be populated -----------------------------------------------

  # Define schema
  schema_output_db <- arrow::schema(
    tempidgeocodebr = arrow::int32(),
    lat = arrow::float16(),  # Equivalent to NUMERIC(8,6)
    lon = arrow::float16(),
    endereco_encontrado = arrow::string(),
    logradouro_encontrado = arrow::string(),
    tipo_resultado = arrow::string(),
    contagem_cnefe = arrow::int32(),
    desvio_metros = arrow::int32()

  )

  if (isTRUE(resultado_completo)) {

    schema_output_db <- arrow::schema(
      tempidgeocodebr = arrow::int32(),
      lat = arrow::float16(),  # Equivalent to NUMERIC(8,6)
      lon = arrow::float16(),
      endereco_encontrado = arrow::string(),
      logradouro_encontrado = arrow::string(),
      tipo_resultado = arrow::string(),
      contagem_cnefe = arrow::int32(),
      desvio_metros = arrow::int32(),
      #
      numero_encontrado = arrow::int32(),
      localidade_encontrada = arrow::string(),
      cep_encontrado = arrow::string(),
      municipio_encontrado = arrow::string(),
      estado_encontrado = arrow::string(),
      similaridade_logradouro = arrow::float16()
    )
  }

  output_db_arrow <- arrow::arrow_table(schema = schema_output_db)
  DBI::dbWriteTableArrow(con, name = "output_db", output_db_arrow,
                         overwrite = TRUE, temporary = TRUE)


  # START MATCHING -----------------------------------------------

  # start progress bar
  if (verboso) {
    prog <- create_progress_bar(input_padrao)
    message_looking_for_matches()
  }

  n_rows <- nrow(input_padrao)
  matched_rows <- 0

  # start matching
  for (match_type in all_possible_match_types ) {

    # get key cols
    key_cols <- get_key_cols(match_type)

    if (verboso) update_progress_bar(matched_rows, match_type)

    # somente busca essa categoria match_type se todas colunas estiverem na base
    # caso contrario, passa para proxima categoria
    if (all(key_cols %in% names(input_padrao))) {

      # select match function
      match_fun <-
        if (match_type %in% c(number_exact_types, exact_types_no_number)) { match_cases
        } else if (match_type %in% number_interpolation_types ) { match_weighted_cases
        } else if (match_type %in% c(probabilistic_exact_types, probabilistic_types_no_number)) { match_cases_probabilistic
        } else if (match_type %in% probabilistic_interpolation_types) { match_weighted_cases_probabilistic
        }

      n_rows_affected <- match_fun(
        con,
        match_type = match_type,
        key_cols = key_cols,
        resultado_completo = resultado_completo
      )

      matched_rows <- matched_rows + n_rows_affected

      # leave the loop early if we find all addresses before covering all cases
      if (matched_rows == n_rows) break
    }

  }

  if (verboso) finish_progress_bar(matched_rows)

  if (verboso) message_preparando_output()

  # add precision column
  add_precision_col(con, update_tb = 'output_db')


  # output with all original columns
  duckdb::dbWriteTable(con, "input_db", enderecos,
                       temporary = TRUE, overwrite=TRUE)
  # enderecos_arrw <- arrow::as_arrow_table(enderecos)
  # DBI::dbWriteTableArrow(con, name = "input_db", enderecos_arrw,
  #                        overwrite = TRUE, temporary = TRUE)

  x_columns <- names(enderecos)

  output_df <- merge_results(
    con,
    x='input_db',
    y='output_db',
    key_column='tempidgeocodebr',
    select_columns = x_columns,
    resultado_completo = resultado_completo
  )

  data.table::setDT(output_df)

  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con)


  # casos de empate -----------------------------------------------
  if (nrow(output_df) > n_rows) {
    output_df <- trata_empates_geocode(output_df, resolver_empates, verboso)
  }

  # drop geocodebr temp id column
  output_df[, tempidgeocodebr := NULL]

  if(isFALSE(resultado_completo)){ output_df[, logradouro_encontrado := NULL]}

  # add H3
  if( !is.null(h3_res) ) {

    colname <- paste0(
      'h3_',
      formatC(h3_res, width = 2, flag = "0")
    )

    output_df[!is.na(lat),
              {{colname}} := h3r::latLngToCell(lat = lat,
                                               lng = lon,
                                               resolution = h3_res)
              ]
    }

  # convert df to simple feature
  if (isTRUE(resultado_sf)) {
    output_sf <- sfheaders::sf_point(
      obj = output_df,
      x = 'lon',
      y = 'lat',
      keep = TRUE
    )

    sf::st_crs(output_sf) <- 4674
    return(output_sf)
  }

  return(output_df[])
}
