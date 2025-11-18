## R CMD check results

── R CMD check results ─────────────────────────────────────────────────────────── geocodebr 0.4.0 ────
Duration: 5m 31s

0 errors ✔ | 0 warnings ✔ | 0 notes ✔


## Mudanças grandes (Major changes)

- A função `geocode()` agora não aplica match probabilístico em lograouros cujo 
nome são só uma letra (e.g. RUA A, RUA B, RUA C) ou compostos só por dígitos 
(RUA 1, RUA 10, RUA 20). [Encerra issue #67](https://github.com/ipeaGIT/geocodebr/issues/67).
Isso diminui muito os casos de falso positivo no match probabilístico.
- O parâmetro `h3_res` utilizado nas funções `geocode()` e `busca_por_cep()` 
agora aceita um vetor de números indicando diferentes resoluções de H3. [Encerra issue #72](https://github.com/ipeaGIT/geocodebr/issues/72).

## Mudanças pequenas (Minor changes)

- Definição de número de `n_cores` para paralelização mais segura usando `{parallelly}`.
- Ganhos de performance em algumas funções de match (issues [#73](https://github.com/ipeaGIT/geocodebr/issues/73), 
[#74](https://github.com/ipeaGIT/geocodebr/issues/74) e [#75](https://github.com/ipeaGIT/geocodebr/issues/75)).
- Tratamento de casos de empate agora é feito interamente dentro do DuckDB. [Encerra issue #57](https://github.com/ipeaGIT/geocodebr/issues/57) 
- O geocodebr não depende mais do pacote Rcpp, que antes era utilizado para 
calcular distâncias entre coordendas. Esses cálculo agora é feito inteiramente
dentro do DuckDB.
