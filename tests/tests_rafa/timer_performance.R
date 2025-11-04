test <- function(){



  ## ---- tiny timing toolkit (self-contained) ------------------------------
  .make_timer <- function(verbose = TRUE) {
    .marks <- list()
    .t0_rt  <- proc.time()[["elapsed"]]     # monotonic wall clock
    .t_prev <- .t0_rt

    fmt <- function(secs) sprintf("%.3f s", secs)

    mark <- function(label) {
      now <- proc.time()[["elapsed"]]
      step  <- now - .t_prev
      total <- now - .t0_rt
      .marks <<- append(.marks, list(list(label = label, step = step, total = total)))
      .t_prev <<- now
      if (verbose) message(sprintf("[%s] +%s (total %s)", label, fmt(step), fmt(total)))
      invisible(now)
    }

    summary <- function(print_summary = verbose) {
      if (length(.marks) == 0) return(invisible(data.frame()))
      df <- data.frame(
        step = vapply(.marks, `[[`, "", "label"),
        step_sec = vapply(.marks, `[[`, 0.0, "step"),
        total_sec = vapply(.marks, `[[`, 0.0, "total"),
        stringsAsFactors = FALSE
      )
      if (print_summary) {
        message("— Timing summary —")
        print(df, row.names = FALSE)
      }
      df
    }

    time_it <- function(label, expr) {
      force(label)
      res <- eval.parent(substitute(expr))
      mark(label)
      invisible(res)
    }

    list(mark = mark, summary = summary, time_it = time_it)
  }
  timer <- .make_timer(verbose = isTRUE(verboso))
  on.exit(timer$summary(), add = TRUE)
  ## -----------------------------------------------------------------------



  timer$mark("Padronização 1")

  Sys.sleep(time = 2)



  timer$mark("Padronização 2")
  Sys.sleep(time = 2)



  timer$mark("Padronização 3")



}
test()




# cad unico 1 milhao de casos

# empate com duckdb2
#                           step step_sec total_sec
#                          Start     0.02      0.02
#                   Padronizacao    17.89     17.91
#    Register standardized input     2.33     20.24
# Cria coluna log_causa_confusao     0.04     20.28
#                       Matching    96.08    116.36
#                  Add precision     0.19    116.55
#                Resolve empates   364.25    480.80
#      Write original input back     0.44    481.24
#                  Merge results     2.78    484.02
#                         Add H3     1.98    486.00
#                  Convert to sf     2.49    488.49

# NAO resolve empate com duckdb2
#                            step step_sec total_sec
#                           Start     0.01      0.01
#                    Padronizacao    18.21     18.22
#     Register standardized input     2.17     20.39
#  Cria coluna log_causa_confusao     0.05     20.44
#                        Matching   100.59    121.03
#                   Add precision     0.17    121.20
#                 Resolve empates     6.13    127.33
#       Write original input back     0.34    127.67
#                   Merge results     3.81    131.48
#                          Add H3     1.86    133.34
#                   Convert to sf     2.99    136.33
