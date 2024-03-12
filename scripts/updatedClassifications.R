source("global.R")
source("utils.R")

drive_download(
  file = "https://docs.google.com/spreadsheets/d/1HqDmZNLubTW0opEEu2W2f9pAwx2MkFsr/edit#gid=36781670",
  path = "data/updated_classifications.xlsx",
  overwrite = TRUE
)


classified_df <- read.xlsx("data/updated_classifications.xlsx")



county <- county_of_interest("isiolo")

lite_conn() %>% 
  dbListTables() %>% as_tibble() %>% 
  filter(value %>% str_ends(county)) %>% pull(value) %>% 
  map_df(
    .,
    ~ dbGetQuery(conn = lite_conn(), glue("SELECT * FROM {.x}"))
  ) -> db_output



db_output %>% 
  filter(funding == "County") %>% 
  left_join(
    classified_df %>% select(-product_or_equipment, -rmnch_product), by = join_by(item_description_name_form_strength, tab)
  ) %>% 
  summarise(
    value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE),
    .by = product_classification
    ) %>% 
  adorn_totals() %>% view()













