# skip tests because they take too much time
skip_if(Sys.getenv("TEST_ONE") != "")
testthat::skip_on_cran()
testthat::skip_if_not_installed("arrow")


ceps_valid <- c("70390-025", "20071-001", "99999-999")
ceps_not_valid <- c("99999-999")


tester <- function(cep,
                   h3_res = NULL,
                   resultado_sf = FALSE,
                   verboso = TRUE,
                   cache = TRUE) {
  busca_por_cep(
    cep,
    h3_res,
    resultado_sf,
    verboso,
    cache
  )
}

test_that("expected output", {

  # expected results
  output <- tester(cep = ceps_valid)
  testthat::expect_true(nrow(output) == 5)

  # expected class
  testthat::expect_s3_class(output, 'data.frame')

  # add h3
  h3_output <- tester(cep = ceps_valid, h3_res = c(3,4))
  testthat::expect_true('h3_03' %in% names(h3_output))
  testthat::expect_true('h3_04' %in% names(h3_output))

  # output in sf format
  sf_output <- tester(cep = ceps_valid, resultado_sf = TRUE)
  testthat::expect_true(is(sf_output , 'sf'))
})



test_that("errors with incorrect input", {
  expect_error(tester(unclass(ceps_not_valid)))

  expect_error(tester(cep = 1))
  expect_error(tester(cep = 'banana'))
  expect_error(tester(cep = ceps_not_valid))

  expect_error(tester(resultado_completo = 1))
  expect_error(tester(resultado_completo = NA))
  expect_error(tester(resultado_completo = c(TRUE, TRUE)))

  expect_error(tester(resultado_sf = 1))
  expect_error(tester(resultado_sf = NA))
  expect_error(tester(resultado_sf = c(TRUE, TRUE)))

  expect_error(tester(verboso = 1))
  expect_error(tester(verboso = NA))
  expect_error(tester(verboso = c(TRUE, TRUE)))

  expect_error(tester(cache = 1))
  expect_error(tester(cache = NA))
  expect_error(tester(cache = c(TRUE, TRUE)))
})

