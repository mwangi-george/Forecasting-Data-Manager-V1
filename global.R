pacman::p_load(
  tidyverse, DBI, RSQLite, googledrive, openxlsx, janitor, fuzzyjoin, here, glue, gt
)

# Drive Authentication
options(
  # whenever there is one account token found, use the cached token
  gargle_oauth_email = Sys.getenv("MY_CORPORATE_EMAIL"),
  # specify auth tokens should be stored in a hidden directory ".secrets"
  gargle_oauth_cache = ".secrets"
)

# Update classifications 
drive_download(
  file = "https://docs.google.com/spreadsheets/d/1HqDmZNLubTW0opEEu2W2f9pAwx2MkFsr/edit#gid=36781670",
  path = "data/product_or_equipment_classification.xlsx",
  overwrite = TRUE
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

