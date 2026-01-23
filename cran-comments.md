## R CMD check results

── R CMD check results ──────────────────────────────────────────────────────────── geocodebr 0.6.0 ────
Duration: 2m 13.9s

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

# geocodebr 0.6.0

## Mudanças grandes (Major changes)

- A função `geocode()` agora retorna o código do setor censitário do endereço 
encontrado quando `resultado_completo = TRUE`. Essa alteração atende parcialmente 
ao [issue #66](https://github.com/ipeaGIT/geocodebr/issues/66) porque ela somente
retorna o código do setor dos casos em que o endeço encontrado está 100% dentro
de um único setor censitário. Quando os dados do CNEFE correspondentes ao endereço
buscado estão em mais de um setor, o resultado da coluna `cod_setor` é `NA`.
- Dependência do pacote agora usa enderecobr (>= 0.5.0), que foi reescrito em 
Rust. Isso traz grandes ganhos de performance para processamento de bases acima 
de 10 milhões
- Nova atualização da da base de referência (CNEFE padronizado v0.4.0)

## Outras novidades (Other news)

- Novo co-autor do pacote: Gabriel Garcia de Almeida



