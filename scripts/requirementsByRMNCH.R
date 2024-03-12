source("global.R")
countyDBCon <- RSQLite::dbConnect(SQLite(), "databases/SCDatabase.db")
countyOfInterest <- "nakuru" %>% # change this to preferred county of interest
  str_to_lower()


tables <- as_tibble(dbListTables(countyDBCon)) %>%
  filter(value %>% str_ends(countyOfInterest)) %>%
  pull(value)

# drive_download(
#   "https://docs.google.com/spreadsheets/d/1HqDmZNLubTW0opEEu2W2f9pAwx2MkFsr/edit#gid=126158597",
#   path = "data/product_or_equipment_classification.xlsx",
#   overwrite = TRUE
# )

# Analysis by Product or equipment
countyOfInterestForecasts <- tables %>% map_df(~ dbGetQuery(countyDBCon, str_c("select * from ", .x)))
productCategorizationData <- read.xlsx("data/product_or_equipment_classification.xlsx", sheet = "EditThis")

countyOfInterestForecasts %>%
  left_join(productCategorizationData, by = join_by(item_description_name_form_strength, tab)) %>% 
  mutate(rmnch_product = case_when(is.na(rmnch_product) ~ "No", .default = rmnch_product)) %>%
  filter(funding == "County") %>%
  summarise(
    metric = sum(value_of_quantities_required_for_12_months, na.rm = TRUE),
    .by = c(tab, rmnch_product)
  ) %>%
  mutate(metric = round(metric/sum(metric), 2), .by = tab) %>% 
  productCategoryCleaner() %>% 
  pivot_wider(names_from = rmnch_product, values_from = metric, values_fill = 0) %>%
  mutate(`% of RMNCH` = str_c(round(Yes / (Yes + No) * 100, 2), "%")) %>% 
  adorn_totals(c("row", "col")) %>% view() 
  
  write.xlsx(str_c("data/", countyOfInterest, "AnalysisByRMNCH.xlsx", sep = ""))


# Disconnect
dbDisconnect(countyDBCon)





