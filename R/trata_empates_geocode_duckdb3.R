# trata_empates_geocode_duckdb3 eh igual a trata_empates_geocode_duckdb2
# mas com menos chamadas de sql. E a performance nao muda

# expression           min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
# DT resolve   744     22s    22s    0.0455    79.4MB    0.455     1    10        22s <dt>   <Rprofmem> <bench_tm>
# Duck resolve 745     22.6s  22.6s  0.0443    57.5MB    0.443     1    10      22.6s <dt>   <Rprofmem> <bench_tm>
# Duck3 resolve745     21.5s  21.5s  0.0466    56.4MB    0.373     1     8      21.5s <dt>   <Rprofmem> <bench_tm>

# expression           min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
# Duck N-resolve 745 22.2s  22.2s    0.0449    55.9MB    0.360     1     8      22.2s <dt>   <Rprofmem> <bench_tm>
# DT N-resolve 745   21.8s  21.8s    0.0459    60.2MB    0.459     1    10      21.8s <dt>   <Rprofmem> <bench_tm>


trata_empates_geocode_duckdb3 <- function(
    con = parent.frame()$con,
    resolver_empates = parent.frame()$resolver_empates,
    verboso = parent.frame()$verboso
) {

  # Haversine macro (kept for speed; consider spatial extension later)
  DBI::dbExecute(con, "
    CREATE MACRO IF NOT EXISTS haversine(lat1, lon1, lat2, lon2) AS (
      6378137 * 2 * ASIN(
        SQRT(
          POWER(SIN(RADIANS(lat2 - lat1) / 2), 2) +
          COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
          POWER(SIN(RADIANS(lon2 - lon1) / 2), 2)
        )
      )
    );
  ")

  # One-shot pipeline
  sql <- "
  -- 0) Base w/ tie-flags + deterministic id inside partition
  WITH base AS (
    SELECT
      *,
      (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate_inicial,
      ROW_NUMBER()
        OVER (PARTITION BY tempidgeocodebr
              ORDER BY contagem_cnefe DESC, desvio_metros, endereco_encontrado) AS id
    FROM output_db
  ),

  -- 1) Distance between consecutive candidates within each tie group
  distd AS (
    SELECT
      b.*,
      CASE WHEN empate_inicial THEN
        haversine(
          lat, lon,
          LEAD(lat) OVER (PARTITION BY tempidgeocodebr ORDER BY id),
          LEAD(lon) OVER (PARTITION BY tempidgeocodebr ORDER BY id)
        )
      END AS dist_geocodebr
    FROM base b
  ),

  -- 2) Keep non-ties and ties where neighbor distance is NULL or > 300m
  filtered AS (
    SELECT
      d.*,
      (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate,
      MAX(dist_geocodebr) OVER (PARTITION BY tempidgeocodebr) AS max_dist
    FROM distd d
    WHERE (empate_inicial IS FALSE)
       OR (empate_inicial AND dist_geocodebr IS NULL)
       OR (empate_inicial AND dist_geocodebr > 300)
  ),

  -- early count used for the fast-return branch
  tie_count AS (
    SELECT COUNT(DISTINCT tempidgeocodebr) AS n FROM filtered WHERE empate
  )

  SELECT * FROM tie_count;
  "

  n_casos_empate <- DBI::dbGetQuery(con, sql)$n[[1]]

  # 2) If no ties left, we’re done
  if (n_casos_empate == 0L) return(TRUE)

  # 3) If not resolving, mark & finish (materialize filtered table once)
  if (isFALSE(resolver_empates)) {
    DBI::dbExecute(con, "
      CREATE OR REPLACE TEMP TABLE output_db2 AS
      SELECT * EXCLUDE (empate_inicial)
      FROM (
        WITH base AS (
          SELECT
            *,
            (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate_inicial,
            ROW_NUMBER() OVER (
              PARTITION BY tempidgeocodebr
              ORDER BY contagem_cnefe DESC, desvio_metros, endereco_encontrado
            ) AS id
          FROM output_db
        ),
        distd AS (
          SELECT
            b.*,
            CASE WHEN empate_inicial THEN
              haversine(
                lat, lon,
                LEAD(lat) OVER (PARTITION BY tempidgeocodebr ORDER BY id),
                LEAD(lon) OVER (PARTITION BY tempidgeocodebr ORDER BY id)
              )
            END AS dist_geocodebr
          FROM base b
        )
        SELECT
          d.*,
          (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate,
          MAX(dist_geocodebr) OVER (PARTITION BY tempidgeocodebr) AS max_dist
        FROM distd d
        WHERE (empate_inicial IS FALSE)
           OR (empate_inicial AND dist_geocodebr IS NULL)
           OR (empate_inicial AND dist_geocodebr > 300)
      );
    ")
    return(TRUE)
  }

  # 4) Resolve ties fully, all in SQL (no R-side id lists)
  sql_resolve <- "
CREATE OR REPLACE TEMP TABLE output_db2 AS
WITH
  base AS (
    SELECT
      *,
      (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate_inicial,
      ROW_NUMBER() OVER (
        PARTITION BY tempidgeocodebr
        ORDER BY contagem_cnefe DESC, desvio_metros, endereco_encontrado
      ) AS id
    FROM output_db
  ),
  distd AS (
    SELECT
      b.*,
      CASE WHEN empate_inicial THEN
        haversine(
          lat, lon,
          LEAD(lat) OVER (PARTITION BY tempidgeocodebr ORDER BY id),
          LEAD(lon) OVER (PARTITION BY tempidgeocodebr ORDER BY id)
        )
      END AS dist_geocodebr
    FROM base b
  ),
  filtered AS (
    SELECT
      d.*,
      (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate,
      MAX(dist_geocodebr) OVER (PARTITION BY tempidgeocodebr) AS max_dist
    FROM distd d
    WHERE (empate_inicial IS FALSE)
       OR (empate_inicial AND dist_geocodebr IS NULL)
       OR (empate_inicial AND dist_geocodebr > 300)
  ),

  -- a) sem empate
  df_sem_empate AS (
    SELECT
      tempidgeocodebr,
      lat,
      lon,
      endereco_encontrado,
      logradouro_encontrado,
      tipo_resultado,
      contagem_cnefe,
      desvio_metros,
      log_causa_confusao,
      numero_encontrado,
      localidade_encontrada,
      cep_encontrado,
      municipio_encontrado,
      estado_encontrado,
      similaridade_logradouro,
      precisao,
      empate
    FROM filtered
    WHERE empate = FALSE
  ),

  -- b) empatados perdidos (exemplo: max_dist > 1000; acrescente suas demais regras aqui)
  df_empates_perdidos AS (
    SELECT
      tempidgeocodebr,
      lat,
      lon,
      endereco_encontrado,
      logradouro_encontrado,
      tipo_resultado,
      contagem_cnefe,
      desvio_metros,
      log_causa_confusao,
      numero_encontrado,
      localidade_encontrada,
      cep_encontrado,
      municipio_encontrado,
      estado_encontrado,
      similaridade_logradouro,
      precisao,
      TRUE AS empate
    FROM filtered
    WHERE empate = TRUE
      AND (
        max_dist > 1000
        OR log_causa_confusao
        OR REGEXP_MATCHES(endereco_encontrado,
            '(RUA (QUATRO|QUATORZE|QUINZE|DEZESSEIS|DEZESSETE|DEZOITO|DEZENOVE|VINTE|TRINTA|QUARENTA|CINQUENTA|SESSENTA|SETENTA|OITENTA|NOVENTA))'
        )
      )
      AND NOT REGEXP_MATCHES(logradouro_encontrado, '\\\\bDE (JANEIRO|FEVEREIRO|MARCO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\\\\b')
    QUALIFY ROW_NUMBER()
      OVER (PARTITION BY tempidgeocodebr ORDER BY contagem_cnefe DESC) = 1
  ),

  -- c) empatados salváveis = restantes (não em a) nem b))
  empates_restantes AS (
    SELECT f.*
    FROM filtered f
    WHERE f.empate = TRUE
      AND NOT EXISTS (SELECT 1 FROM df_sem_empate s WHERE s.tempidgeocodebr = f.tempidgeocodebr)
      AND NOT EXISTS (SELECT 1 FROM df_empates_perdidos p WHERE p.tempidgeocodebr = f.tempidgeocodebr)
  ),
  empates_wavg AS (
    SELECT
      e.*,
      (SUM(lat * contagem_cnefe) OVER (PARTITION BY tempidgeocodebr)
        / NULLIF(SUM(contagem_cnefe) OVER (PARTITION BY tempidgeocodebr), 0)) AS lat_wavg,
      (SUM(lon * contagem_cnefe) OVER (PARTITION BY tempidgeocodebr)
        / NULLIF(SUM(contagem_cnefe) OVER (PARTITION BY tempidgeocodebr), 0)) AS lon_wavg
    FROM empates_restantes e
  ),
  df_empates_salve AS (
    SELECT
      tempidgeocodebr,
      lat_wavg AS lat,
      lon_wavg AS lon,
      endereco_encontrado,
      logradouro_encontrado,
      tipo_resultado,
      contagem_cnefe,
      desvio_metros,
      log_causa_confusao,
      numero_encontrado,
      localidade_encontrada,
      cep_encontrado,
      municipio_encontrado,
      estado_encontrado,
      similaridade_logradouro,
      precisao,
      TRUE AS empate
    FROM empates_wavg
    QUALIFY ROW_NUMBER()
      OVER (PARTITION BY tempidgeocodebr ORDER BY contagem_cnefe DESC) = 1
  )

SELECT
  tempidgeocodebr, lat, lon, endereco_encontrado, logradouro_encontrado,
  tipo_resultado, contagem_cnefe, desvio_metros, log_causa_confusao,
  numero_encontrado, localidade_encontrada, cep_encontrado,
  municipio_encontrado, estado_encontrado, similaridade_logradouro,
  precisao, empate
FROM df_sem_empate
UNION ALL
SELECT
  tempidgeocodebr, lat, lon, endereco_encontrado, logradouro_encontrado,
  tipo_resultado, contagem_cnefe, desvio_metros, log_causa_confusao,
  numero_encontrado, localidade_encontrada, cep_encontrado,
  municipio_encontrado, estado_encontrado, similaridade_logradouro,
  precisao, empate
FROM df_empates_perdidos
UNION ALL
SELECT
  tempidgeocodebr, lat, lon, endereco_encontrado, logradouro_encontrado,
  tipo_resultado, contagem_cnefe, desvio_metros, log_causa_confusao,
  numero_encontrado, localidade_encontrada, cep_encontrado,
  municipio_encontrado, estado_encontrado, similaridade_logradouro,
  precisao, empate
FROM df_empates_salve;
"




  DBI::dbExecute(con, sql_resolve)



  if (verboso) {

  # conta numero de casos empatados
  n_casos_empate <- DBI::dbGetQuery(
    conn = con,
    statement = "SELECT COUNT(DISTINCT tempidgeocodebr)
                 FROM output_db2
                 WHERE empate = TRUE")[[1]]

    plural <- ifelse(n_casos_empate==1, 'caso', 'casos')
    message(glue::glue(
      "Foram encontrados e resolvidos {n_casos_empate} {plural} de empate."
    ))
  }

  return(TRUE)
}
