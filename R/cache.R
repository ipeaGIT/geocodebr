data_release <- "v0.3.0"

listar_pasta_cache_padrao <- function() {
  fs::path(
    tools::R_user_dir("geocodebr", which = "cache")
  )
}

listar_arquivo_config <- function() {
  fs::path(
    tools::R_user_dir("geocodebr", which = "config"),
    "cache_dir"
  )
}

#' Define um diretório de cache para o geocodebr
#'
#' Define um diretório de cache para os dados do geocodebr. Essa configuração
#' é persistente entre sessões do R.
#'
#' @param path Uma string. O caminho para o diretório usado para armazenar os
#'   dados em cache.  Se `NULL`, o pacote usará um diretório versionado salvo
#'   dentro do diretório retornado por [tools::R_user_dir()].
#' @template verboso
#'
#' @return Retorna de forma invisível o caminho do diretório de cache.
#'
#' @examples
#' definir_pasta_cache(tempdir())
#'
#' # retoma pasta padrão do pacote
#' definir_pasta_cache( path = NULL)
#'
#' @export
definir_pasta_cache <- function(path, verboso = TRUE) {
  checkmate::assert_string(path, null.ok = TRUE)
  checkmate::assert_logical(verboso, null.ok = FALSE)

  if (is.null(path)) {
    cache_dir <- listar_pasta_cache_padrao()
  } else {
    cache_dir <- fs::path_norm(path)
  }

  if (isTRUE(verboso)) {
    cli::cli_inform(
      c("i" = "Definido como pasta de cache {.file {cache_dir}}."),
      class = "geocodebr_cache_dir"
    )
  }

  arquivo_config <- listar_arquivo_config()

  if (!fs::file_exists(arquivo_config)) {
    fs::dir_create(fs::path_dir(arquivo_config))
    fs::file_create(arquivo_config)
  }

  cache_dir <- as.character(cache_dir)

  writeLines(cache_dir, con = arquivo_config)

  return(invisible(cache_dir))
}


#' Obtém a pasta de cache usado no geocodebr
#'
#' Obtém o caminho da pasta utilizada para armazenar em cache os dados do
#' geocodebr. Útil para inspecionar a pasta configurada com [definir_pasta_cache()]
#' em uma sessão anterior do R. Retorna a pasta de cache padrão caso nenhuma
#' pasta personalizado tenha sido configurada anteriormente.
#'
#' @return O caminho da pasta de cache.
#'
#' @examples
#' listar_pasta_cache()
#'
#' @export
listar_pasta_cache <- function() {
  arquivo_config <- listar_arquivo_config()

  if (fs::file_exists(arquivo_config)) {
    cache_dir <- readLines(arquivo_config)
    cache_dir <- fs::path_norm(cache_dir)
  } else {
    cache_dir <- listar_pasta_cache_padrao()
  }

  cache_dir <- as.character(cache_dir)

  return(cache_dir)
}

#' Listar dados em cache
#'
#' Lista os dados salvos localmente na pasta de cache
#'
#' @param print_tree Um valor lógico. Indica se o conteúdo da pasta de cache
#'   deve ser exibido em um formato de árvore. O padrão é `FALSE`.
#'
#' @return O caminho para os arquivos em cache
#'
#' @examples
#' listar_dados_cache()
#'
#' listar_dados_cache(print_tree = TRUE)
#'
#' @export
listar_dados_cache <- function(print_tree = FALSE) {
  checkmate::assert_logical(print_tree, any.missing = FALSE, len = 1)

  cache_dir <- listar_pasta_cache()

  if (!fs::dir_exists(cache_dir)) {
    message_cache()
    return(NULL)
  }

  cached_data <- list.files(cache_dir, recursive = TRUE, full.names = TRUE)

  if (print_tree) {
    fs::dir_tree(cache_dir)
    return(invisible(cached_data))
  }

  return(cached_data)
}


#' Deletar pasta de cache do geocodebr
#'
#' Deleta todos arquivos da pasta do cache.
#'
#' @return Retorna de forma invisível o caminho do diretório de cache.
#'
#' @examplesIf identical(TRUE, FALSE)
#' deletar_pasta_cache()
#'
#' @export
deletar_pasta_cache <- function() {
  cache_dir <- listar_pasta_cache()

  unlink(cache_dir, recursive = TRUE)

  message_removed_cache_dir(cache_dir)

  return(invisible(cache_dir))
}

message_removed_cache_dir <- function(cache_dir) {
  geocodebr_message(
    c(
      "v" = "Deletada a pasta de cache que se encontrava em {.path {cache_dir}}."
    )
  )
}


#' Atualiza dados no release local
#'
#' Detecta automaticamente se o release local esta desatualizado. Se sim, apaga
#' a pasta de cache do release local e atualiza a versao do release no caminho
#' da pasta
#'
#' @return Retorna de forma invisível o caminho do diretório de cache.
#'
#' @keywords internal
apaga_data_release_antigo <- function() {
  # list cache local
  cache_dir <- geocodebr::listar_pasta_cache()

  # detect all release paths
  local_release_path <- list.dirs(cache_dir, recursive = T)[-1]
  local_release_path <- local_release_path[grep(
    'geocodebr_data_release_',
    local_release_path
  )]

  new_data_release_dir <- fs::path(
    cache_dir,
    glue::glue("geocodebr_data_release_{data_release}")
  )

  if (identical(as.character(new_data_release_dir), local_release_path)) {
    return(cache_dir)
  }

  # versao numerica dos releases local e do pacote
  local_release <- gsub("[^0-9]", "", basename(local_release_path)) |>
    as.numeric()
  pkg_release <- gsub("[^0-9]", "", data_release) |> as.numeric()

  if (length(local_release) == 0) {
    return(cache_dir)
  }

  if (is.na(local_release) | local_release == pkg_release) {
    return(cache_dir)
  }

  if (local_release != pkg_release) {
    # deleta os dados do release local
    suppressMessages(
      geocodebr::deletar_pasta_cache()
    )

    return(cache_dir)
  }
}
