## R CMD check results

── R CMD check results ────────────────────────────────────────────── geocodebr 0.6.2 ────
Duration: 2m 33.3s

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

# geocodebr v0.6.2

## Correção de bugs (Bug fixes)

- Fixed a bug to ensure that the package uses only cached data from the 
current release and ignores any data from older releases that may be 
in the folder. [Closes #90](https://github.com/ipeaGIT/geocodebr/issues/90)
- The `geocode()` function now returns an informational error when a column in the 
input table has a name containing a non-alphanumeric character, such as . , ? ^ - ! ~. There 
is no issue with the underscore _, as in “name_muni”. Closed [issue #92](https://github.com/ipeaGIT/geocodebr/issues/92)
- Fixed a bug in the `geocode_reverso()` function that prevented the use of very
high values for `dist_max`. [Closes #88](https://github.com/ipeaGIT/geocodebr/issues/88)
- Added ‘Language: pt’ to DESCRIPTION

