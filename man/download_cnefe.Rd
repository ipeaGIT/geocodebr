% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/download_cnefe.R
\name{download_cnefe}
\alias{download_cnefe}
\title{Download the CNEFE data set}
\usage{
download_cnefe(state = "all", progress = TRUE, cache = TRUE)
}
\arguments{
\item{state}{A character vector. The states whose CNEFE data should be
downloaded. Either \code{"all"} (the default), in which case the data for all
states is downloaded, or a vector with the states abbreviations (e.g.
\code{c("RJ", "DF")} to download the data for Rio de Janeiro and the Federal
District).}

\item{progress}{A logical. Whether to display a download progress bar.
Defaults to \code{TRUE}.}

\item{cache}{Logical. Whether the function should read the data cached
locally, which is much faster. Defaults to \code{TRUE}. The first time the
user runs the function, \code{censobr} will download the file and store it
locally so that the file only needs to be download once. If \code{FALSE},
the function will download the data again and overwrite the local file.}
}
\value{
A directory path where the data was saved.
}
\description{
Downloads an enriched version of the CNEFE (National Registry of Addresses
for Statistical Purposes, in portuguese) data set, purposefully built to be
used with this package.
}
\examples{
\dontshow{if (identical(tolower(Sys.getenv("NOT_CRAN")), "true")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
download_cnefe(state = "AC", progress = FALSE)

download_cnefe(state = c("AC", "AL"), progress = FALSE)
\dontshow{\}) # examplesIf}
}
\concept{Support}
