trata_empates_geocode <- function(output_df = parent.frame()$output_df,
                                  resolver_empates = parent.frame()$resolver_empates,
                                  verboso = parent.frame()$verboso) { # nocov start

  # encontra possiveis casos de empate
  data.table::setDT(output_df)[, empate := ifelse(.N > 1, TRUE, FALSE), by = tempidgeocodebr]

  # # calcula distancias entre casos empatados
  output_df[empate == TRUE,
            dist_geocodebr := rcpp_distance_haversine(
              lat, lon,
              data.table::shift(lat, type = "lead"),
              data.table::shift(lon, type = "lead"),
              tolerance = 1e10
              ),
            by = tempidgeocodebr
            ]


  # MANTEM apenas casos de empate que estao a mais de 300 metros
  output_df2 <- output_df[ empate==FALSE |
                           empate==TRUE & dist_geocodebr == 0 |
                           empate==TRUE & dist_geocodebr > 300
                           ]

  # update casos de empate
  output_df2[, empate := ifelse(.N > 1, TRUE, FALSE), by = tempidgeocodebr]

  # conta numero de casos empatados
  ids_empate <- output_df2[empate == TRUE, ]$tempidgeocodebr
  n_casos_empate <- unique(ids_empate) |> length()


  # se nao for para resolver empates:
  # - gera warning
  # - retorna resultado assim mesmo
  if (isFALSE(resolver_empates)) {

    cli::cli_warn(
      "Foram encontrados {n_casos_empate} casos de empate. Estes casos foram
      marcados com valor `TRUE` na coluna 'empate', e podem ser inspecionados na
      coluna 'endereco_encontrado'. Alternativamente, use `resolver_empates = TRUE`
      para que o pacote lide com os empates automaticamente. Ver
      documenta\u00e7\u00e3o da fun\u00e7\u00e3o."
    )
  }



  # se for para resolver empates, trata de 3 casos separados
  # a) nao empatados
  # b) empatados perdidos (dist > 1Km e lograoduros ambiguos)
  #    solucao: usa caso com maior contagem_cnefe
  # c) empatados mas que da pra salvar (dist < 1km e logradouros nao ambiguos)
  #    solucao: agrega casos provaveis de serem na mesma rua com media ponderada
  #    das coordenadas, mas retorna  endereco_encontrado do caso com maior
  #    contagem_cnefe
  # questao documnetada no issue 37

  if (isTRUE(resolver_empates)) {

    # a) casos sem empate
    df_sem_empate <- output_df2[empate == FALSE]
    ids_sem_empate <- df_sem_empate$tempidgeocodebr


    # b) empatados perdidos (dis > 1Km e lograoduros ambiguos)  ---------------------------

    # identifica lograoduros ambiguos (e.g. RUA A)
    num_ext <- c(
      'UM',
      'DOIS',
      'TRES',
      'QUATRO',
      'CINCO',
      'SEIS',
      'SETE',
      'OITO',
      'NOVE',
      'DEZ',
      'ONZE',
      'DOZE',
      'TREZE',
      'QUATORZE',
      'QUINZE',
      'DEZESSEIS',
      'DEZESSETE',
      'DEZOITO' ,
      'DEZENOVE',
      'VINTE',
      'TRINTA',
      'QUARENTA',
      'CINQUENTA',
      'SESSENTA',
      'SETENTA',
      'OITENTA',
      'NOVENTA'
    )

    # ruas_letras <- paste(paste("RUA", LETTERS), collapse = " |")
    # ruas_numerais <- paste(paste("RUA", 1:30), collapse = " |")
    # ruas_letras <- paste0(ruas_letras, " ")
    # ruas_numerais <- paste0(ruas_numerais, " ")

    ruas_num_ext <- paste(paste("RUA", num_ext), collapse = " |")
    ruas_num_ext <- paste0(ruas_num_ext, " ")

    # casos empatados muito distantes

    # ao menos um ponto mais longe q 1Km
    ids_empate_too_distant <- output_df2[empate == TRUE & dist_geocodebr>1000, ]$tempidgeocodebr
    ## a distancia entre 1o e ultimo ponto maior q 1 Km
    # ids_empate_too_distant <- output_df2[empate == TRUE & sum(dist_geocodebr)>1000, ]$tempidgeocodebr

  # tictoc::tic()
  #   empates_perdidos <- output_df2[
  #     empate == TRUE &
  #       (
  #         tempidgeocodebr %in% ids_empate_too_distant |
  #           endereco_encontrado %like% ruas_letras      |
  #           endereco_encontrado %like% ruas_numerais    |
  #           endereco_encontrado %like% ruas_num_ext     |
  #           endereco_encontrado %like% 'ESTRADA|RODOVIA'
  #       )
  #   ]
  #   tictoc::toc()

    empates_perdidos <- output_df2[
      empate == TRUE &
        (
          tempidgeocodebr %in% ids_empate_too_distant |
          endereco_encontrado %like% "^(RUA|TRAVESSA|RAMAL|BECO|BLOCO)\\s+([A-Z]{1,2}|[0-9]{1,3}|[A-Z]{1,2}[0-9]{1,2}|[A-Z]{1,2}\\s+[0-9]{1,2}|[0-9]{1,2}[A-Z]{1,2})\\s+" |
          endereco_encontrado %like% ruas_num_ext     |
          endereco_encontrado %like% 'ESTRADA|RODOVIA'
        )
    ]

    # ainda dah pra salvar enderecos com datas (e.g. 'RUA 15 DE NOVEMBRO')
    meses_pattern <- "\\b\\DE (JANEIRO|FEVEREIRO|MAR\u00c7O|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\\b"
    empates_perdidos <- empates_perdidos[ ! grepl(meses_pattern, logradouro_encontrado) ]

    # selecting the row with max 'contagem_cnefe'
    empates_perdidos <- empates_perdidos[empates_perdidos[, .I[contagem_cnefe == max(contagem_cnefe, na.rm=TRUE)], by = tempidgeocodebr]$V1]
    empates_perdidos <- empates_perdidos[empates_perdidos[, .I[1], by = tempidgeocodebr]$V1]


    # c) casos de empate que podem ser salvos ---------------------------------
    ids_empate_salve <- output_df2[!tempidgeocodebr %in% c(ids_sem_empate, empates_perdidos$tempidgeocodebr)]$tempidgeocodebr
    empates_salve <- output_df2[ tempidgeocodebr %in% ids_empate_salve ]

    if (nrow(empates_salve)>0){
      # check if we have every id TRUE: no id should be left behind
      length(unique(output_df2$tempidgeocodebr)) == sum(
        length(ids_sem_empate),
        length(unique(empates_perdidos$tempidgeocodebr)) ,
        length(unique(ids_empate_salve))
      )

      # calcula media ponderada das coordenadas
      # fica com caso que tem max 'contagem_cnefe'
      empates_salve[, c('lat', 'lon') := list(weighted.mean(lat, w = contagem_cnefe),
                                              weighted.mean(lon, w = contagem_cnefe)
                                              ),
                    by = tempidgeocodebr]

      # selecting the row with max 'contagem_cnefe'
      empates_salve <- empates_salve[empates_salve[, .I[contagem_cnefe == max(contagem_cnefe, na.rm=TRUE)], by = tempidgeocodebr]$V1]
      empates_salve <- empates_salve[empates_salve[, .I[1], by = tempidgeocodebr]$V1]

      }


    # junta tudo
    output_df2 <- data.table::rbindlist(list(df_sem_empate, empates_salve, empates_perdidos))
    output_df2[, 'contagem_cnefe' := NULL]

    # reorder columns
    output_df2 <- output_df2[order(tempidgeocodebr)]

    if (verboso) {
      plural <- ifelse(n_casos_empate==1, 'caso', 'casos')
      message(glue::glue(
        "Foram encontrados e resolvidos {n_casos_empate} {plural} de empate."
      ))
    }
  }

  # drop geocodebr dist columns
  output_df2[, dist_geocodebr := NULL]

  return(output_df2)
} # nocov end






# calculate distances between pairs of coodinates
dt_haversine <- function(lat_from, lon_from, lat_to, lon_to, r = 6378137){ # nocov start
  radians <- pi/180
  lat_to <- lat_to * radians
  lat_from <- lat_from * radians
  lon_to <- lon_to * radians
  lon_from <- lon_from * radians
  dLat <- (lat_to - lat_from)
  dLon <- (lon_to - lon_from)
  a <- (sin(dLat/2)^2) + (cos(lat_from) * cos(lat_to)) * (sin(dLon/2)^2)
  dist <- 2 * atan2(sqrt(a), sqrt(1 - a)) * r
  return(dist)
} # nocov end

# Rcpp::sourceCpp("./src/distance_calcs.cpp")
