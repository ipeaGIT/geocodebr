#' @details Lidando com casos de empate:
#'
#' No processo de geolocalização de dados, é possível que para alguns endereços
#' de input sejam encontrados diferentes coordenadas possíveis (e.g. duas ruas
#' diferentes com o mesmo nome, mas em bairros distintos em uma mesma cidade).
#' Esses casos são trados como empate'. Quando a função `geocode()` recebe o
#' o parâmetro `resolver_empates = TRUE`, os casos de empate são resolvidos
#' automaticamente pela função. A solução destes empates é feita da seguinte
#' maneira:
#'
#' 1) Quando se encontra diferente coordenadas possíveis para um mesmo endereço
#' de input, nós assumimos que essas coordendas pertencem provavelmente a endereços
#' diferentes se (a) estas coordenadas estão a mais de 1Km entre si, ou (b) estão
#' associadas a um logradouro 'ambíguo', i.e. que costumam se repetir em muitos
#' bairros (e.g. "RUA A", "RUA QUATRO", "RUA 10", etc). Nestes casos, a solução
#' de desempate é retornar o ponto com maior número de estabelecimentos no CNEFE,
#' valor indicado na coluna `"contagem_cnefe"`.
#'
#' 2) Quando as coordenadas possivelmente associadas a um endereço estão a menos
#' de 1Km entre si e não se trata de um logradouro 'ambíguo', nós assumimos que
#' os pontos pertencem provavelmente ao mesmo logradouro (e.g. diferentes CEPs
#' ao longo de uma mesma rua). Nestes casos, a solução de desempate é retornar
#' um ponto que resulta da média das coordenadas dos pontos possíveis ponderada
#' pelo valor de `"contagem_cnefe"`. Nesse caso, a coluna de output
#' `"endereco_encontrado"` recebe valor do ponto com maior `"contagem_cnefe"`.
#'
