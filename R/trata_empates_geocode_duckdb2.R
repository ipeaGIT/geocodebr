#
# trata_empates_geocode_duckdb2 <- function(output_df = parent.frame()$output_df,
#                                          con = parent.frame()$con,
#                                          resolver_empates = parent.frame()$resolver_empates,
#                                          verboso = parent.frame()$verboso) {
#
#   # 6666666 isso vai ser desnecessario
#   duckdb::dbWriteTable(con, "output_df", output_df,
#                        temporary = TRUE, overwrite=TRUE)
#
#   # encontra casos de empate (quando tempidgeocodebr se repete)
#   query_encontra_empates <- glue::glue(
#     "ALTER TABLE output_df
#     ADD COLUMN IF NOT EXISTS empate_inicial BOOLEAN DEFAULT FALSE;
#
#      UPDATE output_df AS o
#       SET empate_inicial = TRUE
#       FROM (
#         SELECT tempidgeocodebr, COUNT(*) AS cnt
#         FROM output_df
#         GROUP BY tempidgeocodebr
#       ) AS a
#       WHERE (o.tempidgeocodebr IS NOT DISTINCT FROM a.tempidgeocodebr)  -- matches NULLs too
#         AND a.cnt > 1"
#     )
#   DBI::dbExecute(con, query_encontra_empates)
#
#
#   # a <- DBI::dbReadTable(con, 'output_df')
#   # table(a$empate_inicial)
#   # nrow(a)
#
#   # funcao para calculo de distancia
#   DBI::dbExecute(con, "
#     CREATE MACRO IF NOT EXISTS haversine(lat1, lon1, lat2, lon2) AS (
#       6378137 * 2 * ASIN(
#         SQRT(
#           POWER(SIN(RADIANS(lat2 - lat1) / 2), 2) +
#           COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
#           POWER(SIN(RADIANS(lon2 - lon1) / 2), 2)
#         )
#       )
#     );
#   ")
#
#   # calcula distancias entre casos empatados
#   query_calculate_dist <- glue::glue(
#     "ALTER TABLE output_df ADD COLUMN dist_geocodebr DOUBLE;
#
#      UPDATE output_df AS o
#      SET dist_geocodebr = d.dist_m
#      FROM (
#        SELECT
#          id,
#          CASE
#            WHEN empate_inicial THEN
#              haversine(
#                lat, lon,
#                LEAD(lat) OVER (PARTITION BY tempidgeocodebr ORDER BY id),
#                LEAD(lon) OVER (PARTITION BY tempidgeocodebr ORDER BY id)
#              )
#            ELSE NULL
#          END AS dist_m
#        FROM output_df
#      ) AS d
#      WHERE o.id = d.id;"
#     )
#
#   DBI::dbExecute(con, query_calculate_dist)
#   # a <- DBI::dbReadTable(con, 'output_df')
#   # head(a)
#   # summary(a$dist_geocodebr)
#   # nrow(a) == nrow(output_df)
#
#
#   # MANTEM apenas casos de empate que estao a mais de 300 metros
#   # e atualiza casos de empate
#   query_update_empates <- glue::glue(
#      "CREATE OR REPLACE TABLE output_df2 AS
#       SELECT *,
#         (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate,
#         MAX(dist_geocodebr) OVER (PARTITION BY tempidgeocodebr) AS max_dist
#       FROM output_df
#       WHERE empate_inicial = FALSE
#          OR (empate_inicial = TRUE AND dist_geocodebr IS NULL)
#          OR (empate_inicial = TRUE AND dist_geocodebr > 300);"
#      )
#
#
#
#   DBI::dbExecute(con, query_update_empates)
#   # a <- DBI::dbReadTable(con, 'output_df2')
#   # head(a)
#   # summary(a$dist_geocodebr) 19117
#   # table(a$empate)
#   # table(a$empate_inicial)
#   66666
#   # nrow(output_df)
#   # nrow(a)
#   # 22705 - 22138
#
#   # missing_ids <- setdiff(output_df2$id, a$id)
#   # aa <- output_df2[ id %in% missing_ids]
#
#
#
#   # conta numero de casos empatados
#   n_casos_empate <- DBI::dbGetQuery(
#     conn = con,
#     statement = "SELECT COUNT(DISTINCT tempidgeocodebr)
#                  FROM output_df2
#                  WHERE empate = TRUE")[[1]]
#
#
#   # se nao for para resolver empates:
#   # - gera warning
#   # - retorna resultado assim mesmo
#   if (isFALSE(resolver_empates)) {
#
#     cli::cli_warn(
#       "Foram encontrados {n_casos_empate} casos de empate. Estes casos foram
#       marcados com valor `TRUE` na coluna 'empate', e podem ser inspecionados na
#       coluna 'endereco_encontrado'. Alternativamente, use `resolver_empates = TRUE`
#       para que o pacote lide com os empates automaticamente. Ver
#       documenta\u00e7\u00e3o da fun\u00e7\u00e3o."
#     )
#
#     # fetch result
#     output_df2 <- DBI::dbGetQuery(
#       conn = con,
#       statement = "SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist)
#                    FROM output_df2;"
#       )
#
#     return(output_df2)
#   }
#
#
#   # se for para resolver empates, trata de 3 casos separados
#   # a) nao empatados
#   # b) empatados perdidos (dist > 1Km e lograoduros ambiguos)
#   #    solucao: usa caso com maior contagem_cnefe
#   # c) empatados mas que da pra salvar (dist < 1km e logradouros nao ambiguos)
#   #    solucao: agrega casos provaveis de serem na mesma rua com media ponderada
#   #    das coordenadas, mas retorna  endereco_encontrado do caso com maior
#   #    contagem_cnefe
#   # questao documentada no issue 37
#
#   if (isTRUE(resolver_empates)) {
#
#     # a) casos sem empate (df_sem_empate)
#     query_df_sem_empate <- glue::glue(
#       "CREATE OR REPLACE TABLE df_sem_empate AS
#         SELECT *
#         FROM output_df2
#         WHERE empate = FALSE;"
#       )
#     DBI::dbExecute(con, query_df_sem_empate)
#
#     ids_sem_empate <- DBI::dbGetQuery(
#       conn = con,
#       statement = "SELECT DISTINCT tempidgeocodebr FROM df_sem_empate"
#       )[[1]] |> sort()
#
#
#
#     # b) empatados perdidos (dis > 1Km e lograoduros ambiguos)  ---------------------------
#
#     # identifica logradouros ambiguos (e.g. RUA A)
#     ruas_num_ext <- paste(
#         paste("RUA", c(
#           'UM','DOIS','TRES','QUATRO','CINCO','SEIS','SETE','OITO','NOVE','DEZ',
#           'ONZE','DOZE','TREZE','QUATORZE','QUINZE','DEZESSEIS','DEZESSETE',
#           'DEZOITO','DEZENOVE','VINTE','TRINTA','QUARENTA','CINQUENTA','SESSENTA',
#           'SETENTA','OITENTA','NOVENTA'
#         )),
#         collapse = " |"
#       )
#     ruas_num_ext <- paste0("(", ruas_num_ext, ")")
#
#     # cria tabela com casos perdidos
#     query_df_empates_perdidos <- glue::glue(
#       "CREATE OR REPLACE TABLE df_empates_perdidos AS
#         SELECT *
#           FROM output_df2
#           WHERE empate AND tempidgeocodebr NOT IN ({glue::glue_collapse(ids_sem_empate, sep = ', ')})
#             AND (
#               max_dist > 1000
#               OR REGEXP_MATCHES(endereco_encontrado, '^(RUA|TRAVESSA|RAMAL|BECO|BLOCO)\\\\s+([A-Z]{{1,2}}|[0-9]{{1,3}}|[A-Z]{{1,2}}[0-9]{{1,2}}|[A-Z]{{1,2}}\\\\s+[0-9]{{1,2}}|[0-9]{{1,2}}[A-Z]{{1,2}})\\\\s+')
#               OR REGEXP_MATCHES(endereco_encontrado, '{ruas_num_ext}')
#               OR REGEXP_MATCHES(endereco_encontrado, 'ESTRADA|RODOVIA')
#             )
#             -- ainda dah pra salvar enderecos com datas (e.g. 'RUA 15 DE NOVEMBRO')
#             AND NOT REGEXP_MATCHES(logradouro_encontrado, '\\\\bDE (JANEIRO|FEVEREIRO|MARCO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\\\\b')
#
#       -- selecting the row with max 'contagem_cnefe'
#       QUALIFY ROW_NUMBER()
#            OVER (PARTITION BY tempidgeocodebr
#                  ORDER BY contagem_cnefe DESC) = 1;"
#     )
#
#     DBI::dbExecute(con, query_df_empates_perdidos)
#     # a <- DBI::dbReadTable(con, 'df_empates_perdidos')
#     # head(a)
#
#     ids_empates_perdidos <- DBI::dbGetQuery(
#       conn = con,
#       statement = "SELECT DISTINCT tempidgeocodebr FROM df_empates_perdidos"
#     )[[1]] |> sort()
#
#
#
#     # c) casos de empate que podem ser salvos ---------------------------------
#     ids_ja_resolvidos <- c(ids_sem_empate, ids_empates_perdidos) |> sort()
#
#     ids_empate_salve <- DBI::dbGetQuery(
#       conn = con,
#       statement = glue::glue("SELECT DISTINCT tempidgeocodebr FROM output_df2
#                    WHERE tempidgeocodebr NOT IN ({glue::glue_collapse(ids_ja_resolvidos, sep = ', ')});"
#     ))[[1]] |> sort()
#
#     query_df_empates_salve <- glue::glue(
#       "CREATE OR REPLACE TABLE df_empates_salve AS
#        SELECT * FROM output_df2
#        WHERE tempidgeocodebr IN ({glue::glue_collapse(ids_empate_salve, sep = ', ')});
#
#       -- calcula media ponderada das coordenadas
#       UPDATE df_empates_salve AS o
#       SET lat = d.lat_wavg,
#       lon = d.lon_wavg
#       FROM (
#         SELECT
#         tempidgeocodebr,
#         SUM(lat * contagem_cnefe) / SUM(contagem_cnefe) AS lat_wavg,
#         SUM(lon * contagem_cnefe) / SUM(contagem_cnefe) AS lon_wavg
#         FROM df_empates_salve
#         GROUP BY tempidgeocodebr
#       ) AS d
#       WHERE o.tempidgeocodebr = d.tempidgeocodebr;"
#       )
#
#     DBI::dbExecute(con, query_df_empates_salve)
#     # a <- DBI::dbReadTable(con, 'df_empates_salve')
#     # head(a)
#
#     # fica com caso que tem max 'contagem_cnefe'
#     query_update_coords <- glue::glue(
#       "CREATE OR REPLACE TABLE df_empates_salve AS
#        SELECT * FROM df_empates_salve
#        QUALIFY ROW_NUMBER()
#             OVER (PARTITION BY tempidgeocodebr
#                   ORDER BY contagem_cnefe DESC) = 1;"
#        )
#
#     DBI::dbExecute(con, query_update_coords)
#
#
#     # junta tudo
#     query_junta_tudo <- glue::glue(
#       "SELECT * FROM (
#          SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist) FROM df_sem_empate
#          UNION ALL
#          SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist) FROM df_empates_perdidos
#          UNION ALL
#          SELECT * EXCLUDE (empate_inicial, dist_geocodebr, max_dist) FROM df_empates_salve
#          )
#        ORDER BY tempidgeocodebr;"
#     )
#
#     resultado_df <- DBI::dbGetQuery(con, query_junta_tudo)
#     data.table::setDT(resultado_df)
#
#     n_casos_empate <- resultado_df[empate == TRUE, data.table::uniqueN(tempidgeocodebr)]
#
#     if (verboso) {
#       plural <- ifelse(n_casos_empate==1, 'caso', 'casos')
#       message(glue::glue(
#         "Foram encontrados e resolvidos {n_casos_empate} {plural} de empate."
#       ))
#     }
#
#     return(resultado_df)
#   }
# }
