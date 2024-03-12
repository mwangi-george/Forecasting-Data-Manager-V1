pacman::p_load(
  tidyverse, DBI, RSQLite, googledrive, openxlsx, janitor, fuzzyjoin, here
)

# Drive Authentication
options(
  # whenever there is one account token found, use the cached token
  gargle_oauth_email = Sys.getenv("MY_CORPORATE_EMAIL"),
  # specify auth tokens should be stored in a hidden directory ".secrets"
  gargle_oauth_cache = ".secrets"
)


productCategoryCleaner <- function(x) {
  x %>%
    mutate(
      tab = str_remove(tab, fixed(".")),
      tab = str_remove(tab, "[0-9]"),
      tab = str_remove(tab, "0"),
      tab = str_remove(tab, "1"),
      tab = str_remove(tab, "2")
    )
}

