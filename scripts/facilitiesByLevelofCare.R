source("global.R")
countyOfInterest <- "kakamega" %>% # change this to preferred county of interest
  str_to_lower()


countyDBCon %>%
  dbListTables() %>%
  as_tibble() %>%
  filter(value %>% str_ends(countyOfInterest)) %>%
  pull(value) %>%
  map_df(
    ~ dbGetQuery(
      countyDBCon,
      str_c("select distinct keph_level, facility_name, sub_county from ", .x, " order by keph_level")
    )
  ) %>%
  mutate(
    keph_level = case_when(facility_name %>% str_detect("Merti Sub") ~ "KEPH Level 4", .default = keph_level)
  ) %>%
  view() %>%
  write.xlsx(str_c("data/", countyOfInterest, "FacilitiesByLevelOfCare.xlsx"))



# Disconnect
dbDisconnect(countyDBCon)