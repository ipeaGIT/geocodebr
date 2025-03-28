// Generated by using Rcpp::compileAttributes() -> do not edit by hand
// Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

#include <Rcpp.h>

using namespace Rcpp;

#ifdef RCPP_USE_GLOBAL_ROSTREAM
Rcpp::Rostream<true>&  Rcpp::Rcout = Rcpp::Rcpp_cout_get();
Rcpp::Rostream<false>& Rcpp::Rcerr = Rcpp::Rcpp_cerr_get();
#endif

// rcpp_distance_haversine
Rcpp::NumericVector rcpp_distance_haversine(Rcpp::NumericVector latFrom, Rcpp::NumericVector lonFrom, Rcpp::NumericVector latTo, Rcpp::NumericVector lonTo, double tolerance);
RcppExport SEXP _geocodebr_rcpp_distance_haversine(SEXP latFromSEXP, SEXP lonFromSEXP, SEXP latToSEXP, SEXP lonToSEXP, SEXP toleranceSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< Rcpp::NumericVector >::type latFrom(latFromSEXP);
    Rcpp::traits::input_parameter< Rcpp::NumericVector >::type lonFrom(lonFromSEXP);
    Rcpp::traits::input_parameter< Rcpp::NumericVector >::type latTo(latToSEXP);
    Rcpp::traits::input_parameter< Rcpp::NumericVector >::type lonTo(lonToSEXP);
    Rcpp::traits::input_parameter< double >::type tolerance(toleranceSEXP);
    rcpp_result_gen = Rcpp::wrap(rcpp_distance_haversine(latFrom, lonFrom, latTo, lonTo, tolerance));
    return rcpp_result_gen;
END_RCPP
}

static const R_CallMethodDef CallEntries[] = {
    {"_geocodebr_rcpp_distance_haversine", (DL_FUNC) &_geocodebr_rcpp_distance_haversine, 5},
    {NULL, NULL, 0}
};

RcppExport void R_init_geocodebr(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
