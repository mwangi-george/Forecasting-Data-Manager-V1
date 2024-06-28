source("global.R")
countyDBCon <- RSQLite::dbConnect(SQLite(), "databases/transNzoiaCountyForecastData.db") 

countyOfInterest <- "trans nzoia" %>% # change this to preferred county of interest 
  str_to_lower() 
dbListTables(countyDBCon)


orgUnits <- read_rds("data/orgUnitsKe.rds")

countyOfInterestId <- orgUnits %>% 
  mutate(
    name = name %>% str_remove(" County| Sub County"),
    name = name %>% str_trim()
  ) %>% 
  filter(name == countyOfInterest %>% str_to_title() & parent_id == "HfVjCurKxh2") %>% pull(id)


countyOfInterestDf <- dbListTables(countyDBCon) %>%
  as_tibble() %>%
  filter(value %in% c("Saboti", "Cherangany", "Kwanza", "Kiminini", "Endebess")) %>%
  pull(value) %>%
  map_df(~ dbGetQuery(countyDBCon, str_c("select * from ", .x)))


hierarchyDf <- orgUnits %>%
  filter(
    parent_id %in% c(
      orgUnits %>%
        filter(parent_id %in% c(orgUnits %>% filter(parent_id == countyOfInterestId) %>% pull(id))) %>%
        inner_join(orgUnits %>% filter(parent_id == countyOfInterestId), by = join_by(parent_id == id)) %>% pull(id)
    )
  ) %>%
  inner_join(
    orgUnits %>%
      filter(parent_id %in% c(orgUnits %>% filter(parent_id == countyOfInterestId) %>% pull(id))) %>%
      inner_join(orgUnits %>% filter(parent_id == countyOfInterestId), by = join_by(parent_id == id)),
    by = join_by(parent_id == id)
  ) %>%
  transmute(
    facility_name = name,
    ward = name.x %>% str_remove_all(" Ward"),
    sub_county = name.y %>% str_remove_all(" Sub County")
  ) %>% distinct()


countyOfInterestDf %>%
  left_join(hierarchyDf %>% distinct(), by = join_by(sub_county, facility_name)
  ) %>% 
  mutate(
    ward = case_when(
      # Nakuru facilities where join did not find a match
      facility_name == "Chepsiro Dispensary" ~ "Chepsiro/Kiptoror",
      facility_name == "Kaboleet Dispensary" ~ "Makutano", 
      facility_name == "Endebess Sub County Hospital" ~ "Endebess", 
      facility_name == "Mount Elgon National Park Health Centre" ~ "Endebess", 
      facility_name == "Nabeki Dispensary" ~ "Chepchoina", 
      facility_name == "Mount Elgon Hospital" ~ "Hospital", 
      facility_name == "Matunda Sub County Hospital" ~ "Nabiswa",
      facility_name == "Nazareth Sisters Kipkorion Dispensary" ~ "Kwanza", 
      facility_name == "Sarura Dispensary" ~ "Kwanza", 
      .default = ward
    )
  ) %>% 
  # filter(is.na(ward)) %>% 
  filter(funding == "County") %>%
  mutate(value_of_quantities_required_for_12_months = quantity_required_for_period_specified_above * new_price_kes) %>%
  summarise(
    value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE), 
    .by = c(sub_county, ward, tab)
  ) %>%
  productCategoryCleaner() %>% 
  pivot_wider(names_from = tab, values_from = value_of_quantities_required_for_12_months, values_fill = 0) %>% 
  # adorn_totals(c("row", "col")) %>% distinct(sub_county, ward, .keep_all = TRUE) %>%  view() %>%
  write.xlsx(., here::here(str_c("data/requirementsByWard/", countyOfInterest, "RequirementsByWard.xlsx", sep = "")))







