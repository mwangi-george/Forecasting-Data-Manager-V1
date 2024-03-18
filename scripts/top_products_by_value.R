source("global.R")
source("utils.R")

county <- "isiolo"

lite_conn(county)[[2]] %>% 
  map_df(
    .,
    ~dbGetQuery(conn = lite_conn(county)[[1]], glue("select * from {.x}"))
  ) %>% 
  summarise(
    value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = T), 
    .by = c(tab, item_description_name_form_strength))  %>% 
  productCategoryCleaner() %>% 
  slice_max(value_of_quantities_required_for_12_months, n = 5, by = tab) %>%
  view() %>% 
  write.xlsx(str_c("data/", county, "_top_5_products_by_value.xlsx"))

