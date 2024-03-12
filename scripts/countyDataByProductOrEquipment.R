
source("global.R")
countyDBCon <- RSQLite::dbConnect(SQLite(), "databases/SCDatabase.db")

#old product Categorization
drive_download(
  "https://docs.google.com/spreadsheets/d/1HqDmZNLubTW0opEEu2W2f9pAwx2MkFsr/edit#gid=126158597",
  "data/oldProductCategorization.xlsx", overwrite = TRUE
)

oldProductCategorization <- openxlsx::read.xlsx("data/oldProductCategorization.xlsx", sheet = "EditThis")

unclassifiedProducts <- oldProductCategorization %>% filter(is.na(product_or_equipment))
classifiedProducts <- oldProductCategorization %>% filter(!is.na(product_or_equipment))

# new product Categorization
# drive_download(
#   "https://docs.google.com/spreadsheets/d/15xsvscWrDlFnvwfID3bLVyX9_b7hRZdP/edit#gid=1081874635",
#   "data/newProductCategorization.xlsx", overwrite = TRUE
# )

newProductCategorization <- read_excel("data/newProductCategorization.xlsx") %>% 
  rename(item_description_name_form_strength = item_name, new_ven = ven, tab = product_category)


newProductCategorization %>%
  filter(
    item_description_name_form_strength %in% c(unclassifiedProducts %>% pull(item_description_name_form_strength))
    ) %>% 
  select(item_description_name_form_strength, tab, product_or_equipment, ) %>% 
  bind_rows(classifiedProducts) %>% view() %>% 
  openxlsx::write.xlsx("data/cleanProductCategorization.xlsx")

  
  
  
  
  
  # Disconnect
  dbDisconnect(countyDBCon)

