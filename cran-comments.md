## R CMD check results

── R CMD check results ─────────────────────────────────────────────── geocodebr 0.3.0 ────
Duration: 7m 52.9s

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## Mudanças grandes (Major changes)

- Novo parâmetro `h3_res` nas funções `geocode()` e `busca_por_cep()`, que permite 
o usuário inserir uma coluna no output indicando o id da célula H3 na resolução 
espacial desejada. [Encerra issue #43](https://github.com/ipeaGIT/geocodebr/issues/43).
- O output da função `geocode()` agora inclui uma nova coluna `desvio_metros` que 
apresenta de forma intuitiva o grau de incerteza do resultado encontrado. [Encerra issue #11](https://github.com/ipeaGIT/geocodebr/issues/11).
- Nova base de dados (release `v0.3.0`). A principal mudança aqui foi a 
estratégia de agregação de coordenadas. Na versão anterior, a base consistia numa
média simples das coordenadas dos pontos que pertenciam ao mesmo grupo de colunas.
Na atual versão, esse cálculo é feito em duas etapas. Primeiro encontramos o ponto 
médio e calculamos sua distância até todos os pontos. Em seguida, descartamos 
aqueles pontos que estão acima do percentil 95% de distância, e recalculamos então 
novo ponto médio. Isso evita eventuais distorções quando há poucos pontos muito 
isolados. 
- A nova base de dados (release `v0.3.0`) utiliza arquivos em formato `.parquet`
compactados, o que diminuiu pela metade o tamanho dos arquivos (de `2.98` GB para 
`1.17` GB) e acelera o processo de download dos dados (embora deixa o 
processamento em si ligeiramente mais devagar).
- Os dados de cache agora são armazenados na sub-pasta `"geocodebr_data_release_{data_release}"`, 
dentro da pasta de cache definida pelo usuário. De agora em diante, os dados de 
releases antigos passam a ser deletados automaticamente quando há atualização do 
data release. [Encerra issue #64](https://github.com/ipeaGIT/geocodebr/issues/64).
Mas os dados das versões anteriores `v0.2.0` devem ser apagados manualmente com
a função `deletar_pasta_cache()`.
