% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/geocode_rafa.R
\name{geocode_rafa}
\alias{geocode_rafa}
\title{Geocoding addresses based on CNEFE data}
\usage{
geocode_rafa(
  input_table,
  logradouro = NULL,
  numero = NULL,
  cep = NULL,
  bairro = NULL,
  municipio = NULL,
  estado = NULL,
  progress = TRUE,
  output_simple = TRUE,
  n_cores = NULL,
  cache = TRUE
)
}
\arguments{
\item{input_table}{A data frame.}

\item{logradouro}{A string.}

\item{numero}{A string.}

\item{cep}{A string.}

\item{bairro}{A string.}

\item{municipio}{A string.}

\item{estado}{A string.}

\item{progress}{A logical. Whether to display progress bars when downloading
CNEFE data and when geocoding the addresses. Defaults to \code{TRUE}.}

\item{output_simple}{Logic. Defaults to \code{TRUE}}

\item{n_cores}{A number. The number of cores to be used in parallel
execution. Defaults to 1.}

\item{cache}{A logical. Whether CNEFE data should be saved to/read from
cache, reducing processing time in future calls. Defaults to \code{TRUE}. When
\code{FALSE}, CNEFE data is downloaded to a temporary directory.}
}
\value{
An arrow \code{Dataset} or a \code{"data.frame"} object.
}
\description{
Takes a data frame containing addresses as an input and returns the spatial
coordinates found based on CNEFE data.
}
\examples{
\dontshow{if (identical(tolower(Sys.getenv("NOT_CRAN")), "true")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}

# open input data
data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
input_df <- read.csv(data_path)

df_geo <- geocodebr:::geocode_rafa(
   input_table = input_df,
   logradouro = "nm_logradouro",
   numero = "Numero",
   cep = "Cep",
   bairro = "Bairro",
   municipio = "nm_municipio",
   estado = "nm_uf"
   )
\dontshow{\}) # examplesIf}
}
\concept{Microdata}
