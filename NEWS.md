# geocodebr 0.3.0.9000 dev


## Mudanças pequenas (Minor changes)

- A função `geocode()` agora não aplica match probabilístico em lograouros cujo 
nome são só uma letra (e.g. RUA A, RUA B, RUA C) ou compostos só por dígitos 
(RUA 1, RUA 10, RUA 20). [Encerra issue #7](https://github.com/ipeaGIT/geocodebr/issues/67) 



# geocodebr 0.3.0

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

# geocodebr 0.2.1

## Correção de bugs (Bug fixes)

- Resolvido bug que retornava erro se o input to usuario comecava o geocode direto 
a partir do match case `"pl01"`. [Encerra issue #56](https://github.com/ipeaGIT/geocodebr/issues/56).


# geocodebr 0.2.0

## Mudanças grandes (Major changes)

- A função `geocode()` agora inclui busca com match probabilistico. [Encerra issue #34](https://github.com/ipeaGIT/geocodebr/issues/34).
- Nova função `buscapor_cep()`. [Encerra issue #8](https://github.com/ipeaGIT/geocodebr/issues/8).
- Nova função `geocode_reverso()`. [Encerra issue #35](https://github.com/ipeaGIT/geocodebr/issues/35).
- A função `download_cnefe()` agora aceita o argumento `tabela` para baixar tabelas específicas.

## Mudanças pequenas (Minor changes)

- Ajuste na solução de casos de empate mais refinada e agora detalhada na documentação da função `geocode()`. [Encerra issue #37](https://github.com/ipeaGIT/geocodebr/issues/37). O método adotado na solução de empates agora fica transparente na documentação da função `geocode()`.
- Nova vignette sobre a função `geocode_reverso()`
- Vignette sobre *Get Started* e da função `geocode()` reorganizadas

## Correção de bugs (Bug fixes)

- Resolvido bug que decaracterizava colunas de classe `integer64` na tabela de input de endereços. [Encerra issue #40](https://github.com/ipeaGIT/geocodebr/issues/40).

## Novos contribuidores (New contributions)

- Arthur Bazzolli

# geocodebr 0.1.1

## Correção de bugs

- Corrigido bug na organização de pastas do cache de dados. Fecha o [issue 29](https://github.com/ipeaGIT/geocodebr/issues/29).


# geocodebr 0.1.0

- Primeira versão.
