# Update input_padrao_db to remove observations previously matched

Update input_padrao_db to remove observations previously matched

## Usage

``` r
update_input_db(con, update_tb = "input_padrao_db", reference_tb)
```

## Arguments

- con:

  A db connection

- update_tb:

  String. Name of a table to be updated in con

- reference_tb:

  A table written in con used as reference

## Value

Drops observations from input_padrao_db
