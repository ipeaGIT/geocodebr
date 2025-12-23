## R CMD check results

── R CMD check results ───────────────────────────────────────────────────────────────────────────────────────────────────────────── geocodebr 0.5.0 ────
Duration: 2m 17s

0 errors ✔ | 0 warnings ✔ | 0 notes ✔


## Mudanças grandes (Major changes)

- Novas versões da funções `geocode()`, `geocode_reverso()` e `busca_por_cep()` são 
significamente mais rápidas e usam menos memória RAM. O ganho de eficiência é 
relativamente maior em consultas pequenas. Ver ganhos de performance no issues 
encerrados: [#82](https://github.com/ipeaGIT/geocodebr/issues/82),
[#81](https://github.com/ipeaGIT/geocodebr/issues/81) e [#83](https://github.com/ipeaGIT/geocodebr/issues/83)
- Por padrão, as funções agora recebem `n_cores = NULL`, e o pacote utiliza o 
número máximo de cores físicos disponíveis.
- Agora o argumento `resolver_empates` passa a ser `TRUE` como padrão.

## Mudanças pequenas (Minor changes)

- As tabelas do cnefe agora são registradas na db uma única vez. [Encerra issue #79](https://github.com/ipeaGIT/geocodebr/issues/79).
- O output da função `geocode()` agora é apenas um `"data.frame"`, e não mais um 
`"data.table" "data.frame"`.
- A função `geocode()` passa a ter um novo argumento `padronizar_enderecos` que
indica se os dados de endereço de entrada devem ser padronizados. Por padrão, é 
`TRUE`. Essa padronização é essencial para uma geolocalizaçao correta. Alerta! 
Apenas utilize `padronizar_enderecos = FALSE` caso os dados de input já tenham 
sido padronizados anteriormente com `enderecobr::padronizar_enderecos(..., formato_estados = 'sigla', formato_numeros = 'integer')`. [Encerra issue #68](https://github.com/ipeaGIT/geocodebr/issues/68).
- Incluído o apoio do Instituto Todos pela Saúde (ITpS) no `README` e no arquivo 
`DESCRIPTION`. [Encerra issue #71](https://github.com/ipeaGIT/geocodebr/issues/71).


## Correção de bugs (Bug fixes)

- A função `geocode()` agora é envolta com {callr}, e por isso usa muito menos 
memória RAM e não tem vazamento de memória. [#48](https://github.com/ipeaGIT/geocodebr/issues/48) 
