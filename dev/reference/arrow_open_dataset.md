# Safely use arrow to open a Parquet file

This function handles some failure modes, including if the Parquet file
is corrupted.

## Uso

``` r
arrow_open_dataset(filename)
```

## Argumentos

- filename:

  A local Parquet file

## Valor

An
[`arrow::Dataset`](https://arrow.apache.org/docs/r/reference/Dataset.html)
