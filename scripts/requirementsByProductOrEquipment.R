source("global.R")
countyDBCon <- RSQLite::dbConnect(SQLite(), "databases/SCDatabase.db")
countyOfInterest <- "kakamega" %>% # change this to preferred county of interest
  str_to_lower()


tables <- as_tibble(dbListTables(countyDBCon)) %>%
  filter(value %>% str_ends(countyOfInterest)) %>%
  pull(value)


# Analysis by Product or equipment
countyOfInterestForecasts <- tables %>% map_df(~ dbGetQuery(countyDBCon, str_c("select * from ", .x)))
productCategorizationData <- read_excel("data/cleanProductCategorization.xlsx")

countyOfInterestForecasts %>%
  left_join(productCategorizationData, by = join_by(item_description_name_form_strength, tab)) %>%
  mutate(
    product_or_equipment = case_when(
      is.na(product_or_equipment) & tab == "10.Physiotherapy" ~ "Equipment",
      is.na(product_or_equipment) & tab == "4.Laboratory" ~ "Product",
      is.na(product_or_equipment) & tab == "7.Dental" ~ "Product",
      is.na(product_or_equipment) & tab == "2.Pharmaceuticals" ~ "Product",
      is.na(product_or_equipment) & tab == "6.Radiology" ~ "Product",
      .default = product_or_equipment
    )
  ) %>%
  filter(funding == "County") %>%
  summarise(
    value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE),
    .by = c(tab, product_or_equipment)
  ) %>%
  productCategoryCleaner() %>%
  pivot_wider(names_from = product_or_equipment, values_from = value_of_quantities_required_for_12_months, values_fill = 0) %>%
  adorn_totals(c("row", "col")) %>%
  view() %>% 
  write.xlsx(str_c("data/", countyOfInterest, "AnalysisByProductOrEquipment.xlsx", sep = ""))


# Analysis by VEN
countyOfInterestForecastsWithVen <- countyOfInterestForecasts %>% 
  filter(!is.na(ven))

countyOfInterestForecastsWithoutVen <- countyOfInterestForecasts %>% 
  filter(is.na(ven))

newProductCategorization <- read_excel("data/newProductCategorization.xlsx") %>% 
  rename(item_description_name_form_strength = item_name, new_ven = ven, tab = product_category)

countyOfInterestForecastsWithoutVen %>% 
  left_join(newProductCategorization, by = join_by(item_description_name_form_strength, tab)) %>% 
  mutate(ven = new_ven) %>% 
  select(-c(new_ven, product_or_equipment)) %>% 
  bind_rows(countyOfInterestForecastsWithVen) %>% 
  filter(funding == "County") %>% 
  # mutate(value_of_quantities_required_for_12_months = quantity_required_for_period_specified_above * price_kes) %>% 
  summarise(
    value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE), 
    .by = c(tab, ven)
  ) %>%
  productCategoryCleaner() %>% 
  pivot_wider(names_from = ven, values_from = value_of_quantities_required_for_12_months, values_fill = 0) %>% 
  adorn_totals(c("row", "col")) %>%
  view() %>%
  write.xlsx(str_c("data/", countyOfInterest, "AnalysisByVen.xlsx", sep = ""))



# Disconnect
dbDisconnect(countyDBCon)





