trata_empates_geocode_duckdb3 <- function(con = parent.frame()$con,
                                          resolver_empates = parent.frame()$resolver_empates,
                                          verboso = parent.frame()$verboso) {

  # run everything in one transaction to cut commit/flush overhead
  DBI::dbExecute(con, "BEGIN TRANSACTION")

  on.exit({
    # commit if still open
    DBI::dbExecute(con, "COMMIT")
  }, add = TRUE)

  # 1) Procura empates com menos de 300 metros e corrige de qualquer maneira ------

  # funcao para calculo de distancia (unchanged)
  DBI::dbExecute(
    con,
    "CREATE MACRO IF NOT EXISTS haversine(lat1, lon1, lat2, lon2) AS (
      6378137 * 2 * ASIN(
        SQRT(
          POWER(SIN(RADIANS(lat2 - lat1) / 2), 2) +
          COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
          POWER(SIN(RADIANS(lon2 - lon1) / 2), 2)
        )
      )
    );"
  )

  # cria tabela output_db2 que:
  # - encontra casos de empate (quando tempidgeocodebr se repete)
  # - cria 'id' unico de cada caso de empate
  # - calcula distancias entre casos empatados
  # - ordena tabela output_db2 pelos casos de empate
  DBI::dbExecute(con,
   "CREATE OR REPLACE TEMP TABLE output_db2 AS
    WITH base AS (
      SELECT
        *,
        COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1 AS empate_inicial,
        ROW_NUMBER() OVER (
          PARTITION BY tempidgeocodebr
          ORDER BY contagem_cnefe DESC, desvio_metros, endereco_encontrado
        ) AS id,
        LEAD(lat) OVER (
          PARTITION BY tempidgeocodebr
          ORDER BY contagem_cnefe DESC, desvio_metros, endereco_encontrado
        ) AS lat_lead,
        LEAD(lon) OVER (
          PARTITION BY tempidgeocodebr
          ORDER BY contagem_cnefe DESC, desvio_metros, endereco_encontrado
        ) AS lon_lead
      FROM output_db
    )
    SELECT
      *,
      CASE
        WHEN empate_inicial THEN haversine(lat, lon, lat_lead, lon_lead)
        ELSE NULL
      END AS dist_geocodebr
    FROM base;"
   )

  # a <- DBI::dbReadTable(con, 'output_db2')


  # MANTEM apenas casos de empate que estao a mais de 300 metros
  # e atualiza casos de empate
  DBI::dbExecute(con,
   "CREATE OR REPLACE TEMP TABLE output_db2 AS
    SELECT
      *,
      (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate,
      MAX(dist_geocodebr) OVER (PARTITION BY tempidgeocodebr) AS max_dist
    FROM output_db2
    WHERE empate_inicial = FALSE
       OR dist_geocodebr IS NULL
       OR dist_geocodebr > 300;"
   )

  # conta numero de casos empatados
  n_casos_empate <- DBI::dbGetQuery(
    con,
    "SELECT COUNT(DISTINCT tempidgeocodebr) AS n
     FROM output_db2
     WHERE empate = TRUE"
  )$n



  # 2) se nao tiver mais empates, termina aqui --------------------------------------

  if (n_casos_empate==0) {
    return(TRUE)
  }



  # 3) se nao for para resolver empates: ------------------------------------------
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

    # a <- DBI::dbReadTable(con, "output_db2")
    # data.table::setDT(a)
    # a <- a[order(tempidgeocodebr)]
    # a[,c("lat_lead", "lon_lead") := NULL]
    #
    # DBI::dbRemoveTable(con, "output_db2")
    # return(a)

    return(TRUE)
  }

  # 4 se for para resolver empates, trata de 3 casos separados -----------------

  # a) nao empatados
  # b) empatados perdidos (dist > 1Km e lograoduros ambiguos)
  #    solucao: usa caso com maior contagem_cnefe
  # c) empatados mas que da pra salvar (dist < 1km e logradouros nao ambiguos)
  #    solucao: agrega casos provaveis de serem na mesma rua com media ponderada
  #    das coordenadas, mas retorna  endereco_encontrado do caso com maior
  #    contagem_cnefe
  # questao documentada no issue 37


  ## a) casos sem empate  ----------------------------------

  DBI::dbExecute(con,
   "CREATE OR REPLACE TEMP TABLE df_sem_empate AS
    SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist, id, lat_lead, lon_lead)
    FROM output_db2
    WHERE empate = FALSE;"
   )

  ids_sem_empate <- DBI::dbGetQuery(
    conn = con,
    statement = "SELECT DISTINCT tempidgeocodebr FROM df_sem_empate"
  )[[1]] |> sort()



  ## b) empatados perdidos (dist > 1km OU logradouros ambiguos)  ----------------------------------

  ruas_num_ext <- paste(
    paste("RUA", c(
      'UM','DOIS','TRES','QUATRO','CINCO','SEIS','SETE','OITO','NOVE','DEZ',
      'ONZE','DOZE','TREZE','QUATORZE','QUINZE','DEZESSEIS','DEZESSETE',
      'DEZOITO','DEZENOVE','VINTE','TRINTA','QUARENTA','CINQUENTA','SESSENTA',
      'SETENTA','OITENTA','NOVENTA'
    )),
    collapse = "|"
  )
  ruas_num_ext <- paste0("(", ruas_num_ext, ")")

  DBI::dbExecute(
    con,
    glue::glue(
      "CREATE OR REPLACE TEMP TABLE df_empates_perdidos AS
      SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist, id, lat_lead, lon_lead)
      FROM output_db2
      WHERE empate AND tempidgeocodebr NOT IN ({glue::glue_collapse(ids_sem_empate, sep = ', ')})
        AND (
          max_dist > 1000
          OR REGEXP_MATCHES(logradouro_encontrado,
              '^(RUA|TRAVESSA|RAMAL|BECO|BLOCO|AVENIDA|RODOVIA|ESTRADA)\\\\s+([A-Z]{{1,2}}-?|[0-9]{{1,3}}|[A-Z]{{1,2}}-?[0-9]{{1,3}}|[A-Z]{{1,2}}\\\\s+[0-9]{{1,3}}|[0-9]{{1,3}}-?[A-Z]{{1,2}})(\\\\s+KM( \\\\d+)?)?$')
          OR REGEXP_MATCHES(endereco_encontrado, '{ruas_num_ext}')
        )
        AND NOT REGEXP_MATCHES(logradouro_encontrado,
            '\\\\bDE (JANEIRO|FEVEREIRO|MARCO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\\\\b')
      QUALIFY ROW_NUMBER()
              OVER (PARTITION BY tempidgeocodebr ORDER BY contagem_cnefe DESC) = 1;"
     )
    )

  ids_empates_perdidos <- DBI::dbGetQuery(
    conn = con,
    statement = "SELECT DISTINCT tempidgeocodebr FROM df_empates_perdidos"
  )[[1]] |> sort()



  # c) empatados que podem ser salvos ----------------------------------

  ids_ja_resolvidos <- c(ids_sem_empate, ids_empates_perdidos) |> sort()

  # ids que ainda dah pra salvar
  max_id <- DBI::dbGetQuery(con, "SELECT MAX(tempidgeocodebr) AS max_id FROM output_db2;")
  ids_empate_salve <- setdiff(1:max_id$max_id, ids_ja_resolvidos)

  query_df_empates_salve <- glue::glue(
    "CREATE OR REPLACE TEMP TABLE df_empates_salve AS
       SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist, id, lat_lead, lon_lead)
          FROM output_db2
       WHERE tempidgeocodebr IN ({glue::glue_collapse(ids_empate_salve, sep = ', ')});

      -- calcula media ponderada das coordenadas
      UPDATE df_empates_salve AS o
      SET lat = d.lat_wavg,
      lon = d.lon_wavg
      FROM (
        SELECT
          tempidgeocodebr,
          SUM(lat * contagem_cnefe) / SUM(contagem_cnefe) AS lat_wavg,
          SUM(lon * contagem_cnefe) / SUM(contagem_cnefe) AS lon_wavg
        FROM df_empates_salve
        GROUP BY tempidgeocodebr
        ) AS d
      WHERE o.tempidgeocodebr = d.tempidgeocodebr;"
  )

  DBI::dbSendQueryArrow(con, query_df_empates_salve)
  # a <- DBI::dbReadTable(con, 'df_empates_salve')
  # head(a)

  # fica com caso que tem max 'contagem_cnefe'
  query_update_coords <- glue::glue(
    "CREATE OR REPLACE TEMP TABLE df_empates_salve AS
       SELECT * FROM df_empates_salve
       QUALIFY ROW_NUMBER()
            OVER (PARTITION BY tempidgeocodebr
                  ORDER BY contagem_cnefe DESC) = 1;"
  )

  DBI::dbSendQueryArrow(con, query_update_coords)
  # a <- DBI::dbReadTable(con, 'df_empates_salve')
  # head(a)

  # a <- DBI::dbReadTable(con, "df_sem_empate")
  # b <- DBI::dbReadTable(con, "df_empates_perdidos")
  # c <- DBI::dbReadTable(con, "df_empates_salve")

  # junta casos a b c  ----------------------------------
  # (ORDER BY only where you actually need sorted output)
  DBI::dbExecute(con,
    "CREATE OR REPLACE TABLE output_db2 AS
       SELECT * FROM (
         SELECT * FROM df_sem_empate
         UNION ALL
         SELECT * FROM df_empates_perdidos
         UNION ALL
         SELECT * FROM df_empates_salve
         )
       ORDER BY tempidgeocodebr;"
    )

  # conta numero de casos empatados
  n_casos_empate <- DBI::dbGetQuery(
    conn = con,
    statement = "SELECT COUNT(DISTINCT tempidgeocodebr)
                 FROM output_db2
                 WHERE empate = TRUE")[[1]]

  if (verboso) {
    plural <- ifelse(n_casos_empate == 1, 'caso', 'casos')
    message(glue::glue("Foram encontrados e resolvidos {n_casos_empate} {plural} de empate."))
  }

  # a <- DBI::dbReadTable(con, "output_db2")
  # data.table::setDT(a)
  # a <- a[order(tempidgeocodebr)]
  # DBI::dbRemoveTable(con, "output_db2")
  # return(a)
  # DBI::dbRemoveTable(con, "output_db2")
  return(TRUE)
}
