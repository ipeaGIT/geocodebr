# Message when caching file

Message when caching file

## Usage

``` r
cache_message(
  local_file = parent.frame()$local_file,
  cache = parent.frame()$cache
)
```

## Arguments

- local_file:

  The address of a file passed from the download_file function.

- cache:

  Logical. Whether the cached data should be used.

## Value

A message
