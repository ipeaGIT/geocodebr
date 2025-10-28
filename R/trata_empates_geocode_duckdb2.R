trata_empates_geocode_duckdb2 <- function(con = parent.frame()$con,
                                          resolver_empates = parent.frame()$resolver_empates,
                                          verboso = parent.frame()$verboso) {


  # 1) Procura empates com menos de 300 metros e corrige de qualquer maneira ------

  # funcao para calculo de distancia
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
    );
  ")

  # encontra casos de empate (quando tempidgeocodebr se repete)
  # cria 'id' unico de cada caso de empate
  # ordena tabela output_db pelos casos de empate
  query_encontra_empates_iniciais <- glue::glue(
    "CREATE OR REPLACE TEMP TABLE output_db2 AS
        SELECT *,
          (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate_inicial,
          ROW_NUMBER() OVER (PARTITION BY tempidgeocodebr ORDER BY contagem_cnefe DESC, desvio_metros, endereco_encontrado) AS id
        FROM output_db
        ORDER BY
          CASE WHEN empate_inicial THEN 0 ELSE 1 END,  -- NULL-safe control on `empate_inicial` column
          tempidgeocodebr,
          -contagem_cnefe,
          desvio_metros,
          endereco_encontrado;"
  )

  DBI::dbSendQueryArrow(con, query_encontra_empates_iniciais)
  # a <- DBI::dbReadTable(con, 'output_db2')
  # table(a$empate_inicial)

  # calcula distancias entre casos empatados
  query_calculate_dist <- glue::glue(
    "ALTER TABLE output_db2 ADD COLUMN dist_geocodebr DOUBLE;

     UPDATE output_db2 AS o
     SET dist_geocodebr = d.dist_m
     FROM (
       SELECT
         tempidgeocodebr,
         id,
         CASE
           WHEN empate_inicial THEN
             haversine(
               lat, lon,
               LEAD(lat) OVER (PARTITION BY tempidgeocodebr ORDER BY id),
               LEAD(lon) OVER (PARTITION BY tempidgeocodebr ORDER BY id)
             )
           ELSE NULL
         END AS dist_m
       FROM output_db2
     ) AS d
     WHERE o.tempidgeocodebr = d.tempidgeocodebr AND o.id = d.id;"
  )

  DBI::dbSendQueryArrow(con, query_calculate_dist)
  # b <- DBI::dbReadTable(con, 'output_db2')
  # head(b)
  # summary(b$dist_geocodebr)


  # MANTEM apenas casos de empate que estao a mais de 300 metros
  # e atualiza casos de empate
  query_update_empates <- glue::glue(
    "CREATE OR REPLACE TEMP TABLE output_db2 AS
      SELECT *,
        (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate,
        MAX(dist_geocodebr) OVER (PARTITION BY tempidgeocodebr) AS max_dist
      FROM output_db2
      WHERE empate_inicial = FALSE
         OR (empate_inicial = TRUE AND dist_geocodebr IS NULL)
         OR (empate_inicial = TRUE AND dist_geocodebr > 300);"
  )

  DBI::dbSendQueryArrow(con, query_update_empates)
  # a <- DBI::dbReadTable(con, 'output_db2')
  # b <- DBI::dbReadTable(con, 'output_db2')
  # head(a)
  # summary(a$dist_geocodebr)
  # summary(b$dist_geocodebr)
  # table(a$empate)
  # table(a$empate_inicial)


  # conta numero de casos empatados
  n_casos_empate <- DBI::dbGetQuery(
    conn = con,
    statement = "SELECT COUNT(DISTINCT tempidgeocodebr)
                 FROM output_db2
                 WHERE empate = TRUE")[[1]]

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

    # # fetch result
    # output_db2 <- DBI::dbGetQuery(
    #   conn = con,
    #   statement = "SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist)
    #                FROM output_db2;"
    #   )

    # a <- DBI::dbReadTable(con, "output_db2")
    # data.table::setDT(a)
    # a <- a[order(tempidgeocodebr)]
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

  if (isTRUE(resolver_empates)) {

    ## a) casos sem empate (df_sem_empate) --------------------------------------

    query_df_sem_empate <- glue::glue(
      "CREATE OR REPLACE TEMP TABLE df_sem_empate AS
        SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist, id)
        FROM output_db2
        WHERE empate = FALSE;"
    )

    DBI::dbSendQueryArrow(con, query_df_sem_empate)
    # head( DBI::dbReadTable(con, "df_sem_empate") )

    ids_sem_empate <- DBI::dbGetQuery(
      conn = con,
      statement = "SELECT DISTINCT tempidgeocodebr FROM df_sem_empate"
    )[[1]] |> sort()



    # b) empatados perdidos (dis > 1Km e lograoduros ambiguos)  ----------------

    # identifica logradouros ambiguos (e.g. RUA A)
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

    # cria tabela com casos perdidos
    query_df_empates_perdidos <- glue::glue(
      "CREATE OR REPLACE TEMP TABLE df_empates_perdidos AS
        SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist, id)
          FROM output_db2
          WHERE empate AND tempidgeocodebr NOT IN ({glue::glue_collapse(ids_sem_empate, sep = ', ')})
            AND (
              max_dist > 1000
              OR REGEXP_MATCHES(logradouro_encontrado, '^(RUA|TRAVESSA|RAMAL|BECO|BLOCO|AVENIDA|RODOVIA|ESTRADA)\\\\s+([A-Z]{{1,2}}-?|[0-9]{{1,3}}|[A-Z]{{1,2}}-?[0-9]{{1,3}}|[A-Z]{{1,2}}\\\\s+[0-9]{{1,3}}|[0-9]{{1,3}}-?[A-Z]{{1,2}})(\\\\s+KM( \\\\d+)?)?$')
              OR REGEXP_MATCHES(endereco_encontrado, '{ruas_num_ext}')
            )
            -- ainda dah pra salvar enderecos com datas (e.g. 'RUA 15 DE NOVEMBRO')
            AND NOT REGEXP_MATCHES(logradouro_encontrado, '\\\\bDE (JANEIRO|FEVEREIRO|MARCO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\\\\b')

      -- selecting the row with max 'contagem_cnefe'
      QUALIFY ROW_NUMBER()
           OVER (PARTITION BY tempidgeocodebr
                 ORDER BY contagem_cnefe DESC) = 1;"
    )

    DBI::dbSendQueryArrow(con, query_df_empates_perdidos)
    # a <- DBI::dbReadTable(con, 'df_empates_perdidos')
    # head(a)

    ids_empates_perdidos <- DBI::dbGetQuery(
      conn = con,
      statement = "SELECT DISTINCT tempidgeocodebr FROM df_empates_perdidos"
    )[[1]] |> sort()



    # c) casos de empate que podem ser salvos ----------------------------------

    ids_ja_resolvidos <- c(ids_sem_empate, ids_empates_perdidos) |> sort()

    # ids que ainda dah pra salvar
    max_id <- DBI::dbGetQuery(con, "SELECT MAX(tempidgeocodebr) AS max_id FROM output_db2;")
    ids_empate_salve <- setdiff(1:max_id$max_id, ids_ja_resolvidos)

    query_df_empates_salve <- glue::glue(
      "CREATE OR REPLACE TEMP TABLE df_empates_salve AS
       SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist, id)
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


    # junta tudo
    query_junta_tudo <- glue::glue(
      "CREATE OR REPLACE TABLE output_db2 AS
       SELECT * FROM (
         SELECT * FROM df_sem_empate
         UNION ALL
         SELECT * FROM df_empates_perdidos
         UNION ALL
         SELECT * FROM df_empates_salve
         );"
    )

    DBI::dbSendQueryArrow(con, query_junta_tudo)
    # a <- DBI::dbReadTable(con, 'output_db2')
    # head(a)

    # conta numero de casos empatados
    n_casos_empate <- DBI::dbGetQuery(
      conn = con,
      statement = "SELECT COUNT(DISTINCT tempidgeocodebr)
                 FROM output_db2
                 WHERE empate = TRUE")[[1]]


    if (verboso) {
      plural <- ifelse(n_casos_empate==1, 'caso', 'casos')
      message(glue::glue(
        "Foram encontrados e resolvidos {n_casos_empate} {plural} de empate."
      ))
    }


    # a <- DBI::dbReadTable(con, "output_db2")
    # data.table::setDT(a)
    # a <- a[order(tempidgeocodebr)]
    # DBI::dbRemoveTable(con, "output_db2")
    # return(a)
    # DBI::dbRemoveTable(con, "output_db2")
    return(TRUE)

  }
}

