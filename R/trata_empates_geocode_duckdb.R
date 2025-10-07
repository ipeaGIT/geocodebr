# trata_empates_geocode_duckdb <- function(output_df = parent.frame()$output_df,
#                                          con = parent.frame()$con,
#                                          resolver_empates = parent.frame()$resolver_empates,
#                                          verboso = parent.frame()$verboso) {
#
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
#   duckdb::duckdb_register(
#     con,
#     "output_df",
#     data.table::setDT(output_df)[, geocodebr_row_id := .I],
#     overwrite = TRUE
#   )
#
#   if (isFALSE(resolver_empates)) {
#     sql <- "
#   WITH base AS (
#     SELECT
#       *,
#       COUNT(*) OVER (PARTITION BY tempidgeocodebr) AS n_dups,
#       LEAD(lat) OVER (PARTITION BY tempidgeocodebr ORDER BY geocodebr_row_id) AS next_lat,
#       LEAD(lon) OVER (PARTITION BY tempidgeocodebr ORDER BY geocodebr_row_id) AS next_lon
#     FROM output_df
#   ),
#   dist_calc AS (
#     SELECT
#       *,
#       n_dups > 1 AS empate_inicial,
#       CASE WHEN n_dups > 1 THEN haversine(lat, lon, next_lat, next_lon) END AS dist_geocodebr
#     FROM base
#   ),
#   filtrado AS (
#     SELECT *
#     FROM dist_calc
#     WHERE empate_inicial = FALSE
#        OR dist_geocodebr IS NULL
#        OR dist_geocodebr < 1e-10
#        OR dist_geocodebr > 300
#   ),
#   classificado AS (
#     SELECT
#       *,
#       COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1 AS empate_final
#     FROM filtrado
#   )
#   SELECT
#     c.* EXCLUDE(empate_final, empate_inicial, geocodebr_row_id, dist_geocodebr, next_lat, next_lon, n_dups),
#     c.empate_final AS empate
#   FROM classificado c
#   ORDER BY tempidgeocodebr;
# "
#     resultado_df <- DBI::dbGetQuery(con, sql)
#
#   } else {
#     ruas_num_ext <- paste(
#       paste("RUA", c(
#         'UM','DOIS','TRES','QUATRO','CINCO','SEIS','SETE','OITO','NOVE','DEZ',
#         'ONZE','DOZE','TREZE','QUATORZE','QUINZE','DEZESSEIS','DEZESSETE',
#         'DEZOITO','DEZENOVE','VINTE','TRINTA','QUARENTA','CINQUENTA','SESSENTA',
#         'SETENTA','OITENTA','NOVENTA'
#       )),
#       collapse = " |"
#     )
#     ruas_num_ext <- paste0("(", ruas_num_ext, ")")
#
#     sql <- glue::glue("
#       WITH base AS (
#         SELECT
#           *,
#           COUNT(*) OVER (PARTITION BY tempidgeocodebr) AS n_dups,
#           LEAD(lat) OVER (PARTITION BY tempidgeocodebr ORDER BY geocodebr_row_id) AS next_lat,
#           LEAD(lon) OVER (PARTITION BY tempidgeocodebr ORDER BY geocodebr_row_id) AS next_lon
#         FROM output_df
#       ),
#       dist_calc AS (
#         SELECT
#           *,
#           n_dups > 1 AS empate,
#           CASE WHEN n_dups > 1 THEN haversine(lat, lon, next_lat, next_lon) END AS dist_geocodebr
#         FROM base
#       ),
#       filtrado AS (
#         SELECT *
#         FROM dist_calc
#         WHERE empate = FALSE OR dist_geocodebr IS NULL OR dist_geocodebr < 1e-10 OR dist_geocodebr > 300
#       ),
#       classificado AS (
#         SELECT
#           *,
#           COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1 AS empate_final,
#           MAX(dist_geocodebr) OVER (PARTITION BY tempidgeocodebr) AS max_dist
#         FROM filtrado
#       ),
#       empates_perdidos AS (
#         SELECT *
#         FROM classificado
#         WHERE empate_final
#           AND (
#             max_dist > 1000
#             OR REGEXP_MATCHES(endereco_encontrado, '^(RUA|TRAVESSA|RAMAL|BECO|BLOCO)\\\\s+([A-Z]{{1,2}}|[0-9]{{1,3}}|[A-Z]{{1,2}}[0-9]{{1,2}}|[A-Z]{{1,2}}\\\\s+[0-9]{{1,2}}|[0-9]{{1,2}}[A-Z]{{1,2}})\\\\s+')
#             OR REGEXP_MATCHES(endereco_encontrado, '{ruas_num_ext}')
#             OR REGEXP_MATCHES(endereco_encontrado, 'ESTRADA|RODOVIA')
#           )
#           AND NOT REGEXP_MATCHES(logradouro_encontrado, '\\\\bDE (JANEIRO|FEVEREIRO|MARÇO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\\\\b')
#         QUALIFY ROW_NUMBER() OVER (PARTITION BY tempidgeocodebr ORDER BY contagem_cnefe DESC, geocodebr_row_id) = 1
#       ),
#       empates_salve AS (
#         SELECT
#           tempidgeocodebr,
#           SUM(lat * contagem_cnefe) / SUM(contagem_cnefe) AS new_lat,
#           SUM(lon * contagem_cnefe) / SUM(contagem_cnefe) AS new_lon,
#           MAX_BY(geocodebr_row_id, contagem_cnefe) AS geocodebr_row_id,
#           TRUE AS empate_final
#         FROM classificado
#         WHERE empate_final AND tempidgeocodebr NOT IN (SELECT tempidgeocodebr FROM empates_perdidos)
#         GROUP BY tempidgeocodebr
#       ),
#       decisoes AS (
#         SELECT geocodebr_row_id, NULL AS new_lat, NULL AS new_lon, FALSE AS empate_final
#         FROM classificado
#         WHERE NOT empate_final
#         QUALIFY ROW_NUMBER() OVER (PARTITION BY tempidgeocodebr ORDER BY geocodebr_row_id) = 1
#
#         UNION ALL
#         SELECT geocodebr_row_id, NULL AS new_lat, NULL AS new_lon, TRUE AS empate_final
#         FROM empates_perdidos
#
#         UNION ALL
#         SELECT geocodebr_row_id, new_lat, new_lon, TRUE AS empate_final
#         FROM empates_salve
#       )
#       SELECT
#         o.* EXCLUDE(geocodebr_row_id, lat, lon, contagem_cnefe),
#         COALESCE(d.new_lat, o.lat) AS lat,
#         COALESCE(d.new_lon, o.lon) AS lon,
#         o.contagem_cnefe,
#         d.empate_final AS empate
#       FROM output_df o
#       JOIN decisoes d USING (geocodebr_row_id);
#     ")
#
#     resultado_df <- DBI::dbGetQuery(con, sql)
#   }
#
#   resultado_df <- data.table::setDT(resultado_df)
#   resultado_df <- resultado_df[order(tempidgeocodebr)]
#   n_casos_empate <- resultado_df[empate == TRUE, data.table::uniqueN(tempidgeocodebr)]
#   if ("geocodebr_row_id" %in% names(resultado_df)) resultado_df[, geocodebr_row_id := NULL]
#   if ("empate_final" %in% names(resultado_df)) resultado_df[, empate_final := NULL]
#   if ("contagem_cnefe" %in% names(resultado_df) && resolver_empates) resultado_df[, contagem_cnefe := NULL]
#
#   if (!resolver_empates && n_casos_empate > 0) {
#     cli::cli_warn("Foram encontrados {n_casos_empate} casos de empate. Estes casos foram
#       marcados com valor `TRUE` na coluna 'empate', e podem ser inspecionados na
#       coluna 'endereco_encontrado'. Alternativamente, use `resolver_empates = TRUE`
#       para que o pacote lide com os empates automaticamente. Ver
#       documenta\u00e7\u00e3o da fun\u00e7\u00e3o.")
#   } else if (resolver_empates && verboso && n_casos_empate > 0) {
#     plural <- ifelse(n_casos_empate == 1, "caso", "casos")
#     message(glue::glue("Foram encontrados e resolvidos {n_casos_empate} {plural} de empate."))
#   }
#
#   return(resultado_df[])
# }
