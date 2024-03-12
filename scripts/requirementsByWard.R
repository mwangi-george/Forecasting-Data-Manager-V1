source("global.R")
countyDBCon <- RSQLite::dbConnect(SQLite(), "databases/SCDatabase.db") # this is 
countyOfInterest <- "kakamega" %>% # change this to preferred county of interest 
  str_to_lower() 

orgUnits <- read_rds("data/orgUnitsKe.rds")

countyOfInterestId <- orgUnits %>% 
  mutate(
    name = name %>% str_remove(" County| Sub County"),
    name = name %>% str_trim()
  ) %>% 
  filter(name == countyOfInterest %>% str_to_title() & parent_id == "HfVjCurKxh2") %>% pull(id)


countyOfInterestDf <- dbListTables(countyDBCon) %>%
  as_tibble() %>%
  filter(str_ends(value, countyOfInterest)) %>%
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
  # filter(is.na(ward)) %>% distinct(facility_name) %>% arrange(facility_name) %>% view()
  mutate(
    ward = case_when(
      # Nakuru facilities where join did not find a match
      facility_name == "Kabati Dispensary (Ymca)" ~ "Viwandani",
      facility_name == "Mauche Health Center" ~ "Mauche", 
      facility_name == "Neissuit Health Center" ~ "Nessuit", 
      facility_name == "Kihingo Dispensary (Cdf)" ~ "Kihingo", 
      facility_name == "Mwigito. Dispensary" ~ "Kihingo", 
      facility_name == "Sururu Dispensary (CDF)" ~ "Mau Narok", 
      facility_name == "Kapkures Health Centre (Nakuru West)" ~ "Kapkures", 
      facility_name == "Rajuera Dispensary" ~ "Visoi", 
      facility_name == "Rongai Turi Dispensary" ~ "Mosop", 
      facility_name == "Lomolo Dispensary" ~ "Soin", 
      facility_name == "Losibil Dispensary" ~ "Soin", 
      facility_name == "Munanda Dispensary" ~ "Mbaruk/Eburu", 
      facility_name == "Kigoror Dispensary" ~ "Barut", 
      
      
      # Kakamega facilities where join did not find a match
      facility_name == "Eshibembe Health Centre" ~ "Eshibembe Health Centre", 
      facility_name == "Emungabo Dispensary" ~ "Kisa Central",
      facility_name == "Khwisero Level Iv Hospital" ~ "Kisa North",
      facility_name == "Mbururu dispensary" ~ "Nzoia",
      facility_name == "Lumakanda County Hospital" ~ "Lumakanda",
      facility_name == "Gk Prisons Dispensary (Kakamega Central)" ~ "Shirere",
      facility_name == "Mutingongo Dispensary" ~ "Chemuche",
      facility_name == "Shieywe dispensary" ~ "Shirungu-mugai",
      facility_name == "Silungai dispensary" ~ "Manda-shivanga",
      facility_name == "Lunganyiro Health Centre" ~ "Namamali",
      facility_name == "Mungungu Dispensary" ~ "Koyonzo",
      facility_name == "Khaunga Health centre" ~ "East Wanga",
      facility_name == "Kamashia Health Centre" ~ "Lusheya/Lubinu",
      facility_name == "Munganga Health Centre" ~ "East Wanga",
      facility_name == "Wangnyang Dispensary" ~ "Etenje",
      facility_name == "Mumias Level Iv Hospital" ~ "Mumias Central",
      facility_name == "Mutaho Dispensary" ~ "Idakho North",
      
      
      # Isiolo facilities where join did not find a match
      facility_name == "Garbatulla Sub County Hospital" ~ "Garba Tulla",
      facility_name == "Apu Dispensary" ~ "Burat",
      facility_name == "Kiwanja Ndege Dispensary" ~ "Wabera",
      facility_name == "Gk Prison Dispensary (Isiolo)" ~ "Wabera",
      facility_name == "Tuale Dispensary Isiolo" ~ "Oldo/Nyiro",
      facility_name == "Gotu Dispensary" ~ "Ngare Mara",
      facility_name == "Waso Aipca Dispensary" ~ "Bulla Pesa",
      facility_name == "Mata-Arba Dispensary" ~ "Cherab",
      facility_name == "Merti Sub County Hospital" ~ "Cherab",
      .default = ward
    )
  ) %>% 
  
  filter(funding == "County") %>% 
  # mutate(value_of_quantities_required_for_12_months = quantity_required_for_period_specified_above * price_kes) %>% 
  summarise(
    value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE), 
    .by = c(sub_county, ward, tab)
  ) %>%
  productCategoryCleaner() %>% 
  pivot_wider(names_from = tab, values_from = value_of_quantities_required_for_12_months, values_fill = 0) %>%
  # adorn_totals(c("row", "col")) %>% distinct(sub_county, ward, .keep_all = TRUE) %>%  view() %>%
  write.xlsx(., here::here(str_c("data/requirementsByWard/", countyOfInterest, "RequirementsByWard.xlsx", sep = "")))




# Disconnect
dbDisconnect(countyDBCon)
