source("global.R")
transnzoiaCon <- dbConnect(SQLite(), "databases/transNzoiaCountyForecastData.db")
countyDBCon <- RSQLite::dbConnect(SQLite(), "databases/SCDatabase.db")

dbListTables(transnzoiaCon)

transnzoiaSubCounties <- c("Saboti", "Kwanza", "Kiminini", "Endebess", "Cherangany")

query <- "SELECT * FROM "
allTransNzoiaData <- map_df(transnzoiaSubCounties, ~dbGetQuery(transnzoiaCon, str_c(query, .x))) 

# head(allTransNzoiaData) %>% view()
productCategorizationData <- read_excel("data/cleanProductCategorization.xlsx")
transnzoiaNewlyClassifiedProducts <- read_excel("data/transNzoiaUnclassifiedProducts.xlsx")

allTransNzoiaData %>%
  mutate(
    id = consecutive_id(item_description_name_form_strength),
    ven = case_when(!ven %in% c("E", "V", "N") ~ NA, .default = ven)
    ) %>% 
  relocate(id, .before = 1) %>% 
  left_join(productCategorizationData, by = join_by(item_description_name_form_strength, tab)) %>% 
  mutate(
    product_or_equipment = case_when(is.na(product_or_equipment) ~ "Product", .default = product_or_equipment),
    value_of_quantities_required_for_12_months = quantity_required_for_period_specified_above * new_price_kes,
    value_of_quantities_required_including_buffer_kes = quantity_required_buffer_packs * new_price_kes,
    value_of_quantities_to_be_procured_kes = quantity_required_to_procure_packs * new_price_kes
  ) %>% 
  filter(funding == "County") %>% 
  summarise(
    value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE),
    #value_of_quantities_required_including_buffer_kes = sum(value_of_quantities_required_including_buffer_kes, na.rm = TRUE),
    #value_of_quantities_to_be_procured_kes = sum(value_of_quantities_to_be_procured_kes, na.rm = TRUE),
    .by = c(tab, product_or_equipment)
  ) %>% 
  productCategoryCleaner() %>% 
  pivot_wider(names_from = product_or_equipment, values_from = value_of_quantities_required_for_12_months, values_fill = 0) %>% 
  # adorn_totals(c("row", "col")) #%>%
  write.xlsx("data/transNzoiaAnalysisByConsumablesOrEquipment.xlsx")




newProductCategorization <- read_excel("data/newProductCategorization.xlsx") %>% 
  rename(item_description_name_form_strength = item_name, new_ven = ven, tab = product_category)


# facilities per level of care in trans nzoia
transNzoiaDf1 <- allTransNzoiaData %>%
  mutate(
    ven = case_when(!ven %in% c("E", "V", "N") ~ NA, .default = ven)
  ) %>% 
  filter(!is.na(ven))
 
transNzoiaDf2 <- allTransNzoiaData %>%
  mutate(
    ven = case_when(!ven %in% c("E", "V", "N") ~ NA, .default = ven)
  ) %>% 
  filter(is.na(ven))

transNzoiaDf2 %>% 
  left_join(newProductCategorization, by = join_by(item_description_name_form_strength, tab)) %>%
  mutate(ven = new_ven) %>% 
  select(-c(new_ven, product_or_equipment)) %>% 
  bind_rows(transNzoiaDf1) %>% 
  mutate(
    #product_or_equipment = case_when(is.na(product_or_equipment) ~ "Product", .default = product_or_equipment),
    value_of_quantities_required_for_12_months = quantity_required_for_period_specified_above * new_price_kes,
    value_of_quantities_required_including_buffer_kes = quantity_required_buffer_packs * new_price_kes,
    value_of_quantities_to_be_procured_kes = quantity_required_to_procure_packs * new_price_kes
  ) %>% 
  filter(funding == "County") %>% 
  summarise(
    value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE),
    #value_of_quantities_required_including_buffer_kes = sum(value_of_quantities_required_including_buffer_kes, na.rm = TRUE),
    #value_of_quantities_to_be_procured_kes = sum(value_of_quantities_to_be_procured_kes, na.rm = TRUE),
    .by = c(tab, ven)
  ) %>% 
  productCategoryCleaner() %>% 
  filter(!is.na(ven)) %>% 
  pivot_wider(names_from = ven, values_from = value_of_quantities_required_for_12_months, values_fill = 0) %>% 
  # adorn_totals(c("row", "col")) %>%
  write.xlsx("data/transNzoiaAnalysisByVen.xlsx")
  


# Disconnect db
dbDisconnect(countyDBCon)






