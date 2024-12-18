% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cache.R
\name{get_cache_dir}
\alias{get_cache_dir}
\title{Get the cache directory used in geocodebr}
\usage{
get_cache_dir()
}
\value{
The cache directory path.
}
\description{
Gets the directory used to cache CNEFE data. Useful to inspect the directory
set with \code{\link[=set_cache_dir]{set_cache_dir()}} in a previous R session. Returns the default cache
directory if no custom directory has been previously set.
}
\examples{
get_cache_dir()

}
