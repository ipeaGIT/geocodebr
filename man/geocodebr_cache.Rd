% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/geocodebr_cache.R
\name{geocodebr_cache}
\alias{geocodebr_cache}
\title{Manage cached files from the geocodebr package}
\usage{
geocodebr_cache(list_files = TRUE, delete_file = NULL, silent = FALSE)
}
\arguments{
\item{list_files}{Logical. Whether to print a message with the address of all
geocodebr data sets cached locally. Defaults to \code{TRUE}.}

\item{delete_file}{String. The file name (basename) of a geocodebr data set
cached locally that should be deleted. Defaults to \code{NULL}, so that no
file is deleted. If \code{delete_file = "all"}, then all cached geocodebr
files are deleted.}

\item{silent}{Logical. Defaults to \code{TRUE} so messages are printed.}
}
\value{
A message indicating which file exist and/or which ones have been
deleted from local cache directory.
}
\description{
Manage cached files from the geocodebr package
}
\examples{
\dontshow{if (identical(tolower(Sys.getenv("NOT_CRAN")), "true")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
# list all files cached
geocodebr_cache(list_files = TRUE)

# delete particular file
geocodebr_cache(delete_file = '2010_deaths')
\dontshow{\}) # examplesIf}
}
\seealso{
Other Cache data: 
\code{\link{set_geocodebr_cache_dir}()}
}
\concept{Cache data}
