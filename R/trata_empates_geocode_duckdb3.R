# trata_empates_geocode_duckdb3 eh igual a trata_empates_geocode_duckdb2
# mas com menos chamadas de sql. E a performance nao muda

# expression           min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
# DT    resolve 744    22s    22s   0.0455    79.4MB    0.455     1    10        22s <dt>   <Rprofmem> <bench_tm>
# Duck2 resolve 745  22.6s  22.6s   0.0443    57.5MB    0.443     1    10      22.6s <dt>   <Rprofmem> <bench_tm>
# Duck3 resolve 745  21.5s  21.5s   0.0466    56.4MB    0.373     1     8      21.5s <dt>   <Rprofmem> <bench_tm>

# expression           min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
# Duck N-resolve 745 22.2s  22.2s    0.0449    55.9MB    0.360     1     8      22.2s <dt>   <Rprofmem> <bench_tm>
# DT N-resolve 745   21.8s  21.8s    0.0459    60.2MB    0.459     1    10      21.8s <dt>   <Rprofmem> <bench_tm>


trata_empates_geocode_duckdb3 <- function(
    con = parent.frame()$con,
    resolver_empates = parent.frame()$resolver_empates,
    verboso = parent.frame()$verboso
    ){


  # 1) checa se tem empates --------------------------------------

  n_casos_empate <- DBI::dbGetQuery(
    conn = con,
    statement = "SELECT COUNT(*) AS n_casos_empate
                    FROM (
                      SELECT tempidgeocodebr
                      FROM output_db
                      GROUP BY tempidgeocodebr
                      HAVING COUNT(*) > 1
                    ) AS repeated;")[[1]]



  # 2) se nao tiver mais empates, termina aqui --------------------------------------
  if (n_casos_empate == 0) { return(n_casos_empate) }

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

    return(0)
  }

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



  # 3 se for para resolver empates, trata de 3 casos separados -----------------
  # D) nao empatados
  # E) empatados perdidos (dist > 1Km e lograoduros ambiguos)
  #    solucao: usa caso com maior contagem_cnefe
  # F) empatados mas que da pra salvar (dist < 1km e logradouros nao ambiguos)
  #    solucao: agrega casos provaveis de serem na mesma rua com media ponderada
  #    das coordenadas, mas retorna  endereco_encontrado do caso com maior
  #    contagem_cnefe
  # questao documentada no issue 37

  sql_resolve <- "
      CREATE OR REPLACE TEMP TABLE output_db2 AS

      -- A) tabela *base* calculate empates iniciais (inclui mesmo aqueles a menos de 300 metros)
      WITH
        base AS (
          SELECT
            *,
            (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate_inicial,
            ROW_NUMBER() OVER (PARTITION BY tempidgeocodebr ORDER BY contagem_cnefe DESC, desvio_metros, endereco_encontrado) AS id
          FROM output_db
        ),

      -- B) tabela *distd* calculate distancia entre os casos empatados
      distd AS (
          SELECT
            b.*,
            CASE WHEN empate_inicial THEN
              haversine(
                lat, lon,
                LEAD(lat) OVER (PARTITION BY tempidgeocodebr ORDER BY id),
                LEAD(lon) OVER (PARTITION BY tempidgeocodebr ORDER BY id)
              )
            END AS dist_geocodebr_metros
          FROM base b
        ),

      -- C) tabela *filtered* pra manter apenas casos de empate que estao a mais de 300 metros e atualiza coluna de empate
      filtered AS (
          SELECT
            d.*,
            (COUNT(*) OVER (PARTITION BY tempidgeocodebr) > 1) AS empate,
            MAX(dist_geocodebr_metros) OVER (PARTITION BY tempidgeocodebr) AS max_dist
          FROM distd d
          WHERE (empate_inicial IS FALSE)
             OR (empate_inicial AND dist_geocodebr_metros IS NULL)
             OR (empate_inicial AND dist_geocodebr_metros > 300)
        ),

      -- D) tabela *df_sem_empate* com os casos sem empate
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

        -- E) empatados perdidos (exemplo: max_dist > 1000; acrescente demais regras aqui)
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

        -- F) empatados salvaveis = restantes (não em a) nem b))
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

      -- junta as 3 tabelas numa soh (df_sem_empate, df_empates_perdidos, df_empates_salve)
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

  return(n_casos_empate)
}
