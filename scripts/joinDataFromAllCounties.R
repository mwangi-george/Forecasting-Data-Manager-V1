source("global.R")

product_classifications <- read.xlsx("data/product_or_equipment_classification.xlsx")
# product_classifications %>% view()
transnzoiaCon <- dbConnect(SQLite(), "databases/transNzoiaCountyForecastData.db")
countyDBCon <- RSQLite::dbConnect(SQLite(), "databases/SCDatabase.db")
masterCon <- dbConnect(SQLite(), "databases/master_for_data_collection.db")


dbListTables(transnzoiaCon)
dbListTables(countyDBCon)
dbListTables(masterCon)
dbReadTable(masterCon, "master") %>% distinct(county)

countyMasters <- dbReadTable(masterCon, "master") %>% 
  group_by(county) %>% 
  group_split()

countyMasters[[1]] %>% head() %>% view()

transnzoiaSubCounties <- c("Saboti", "Kwanza", "Kiminini", "Endebess", "Cherangany")


query <- "SELECT * FROM "
allTransNzoiaData <- map_df(transnzoiaSubCounties, ~ dbGetQuery(transnzoiaCon, str_c(query, .x)))
allTransNzoiaData %>% dim()
allTransNzoiaData %>% head() %>% view()

colsTrans <- names(allTransNzoiaData)

allOtherCountiesData <- dbListTables(countyDBCon) %>%
  map_df(~ dbGetQuery(countyDBCon, str_c(query, .x)))

data_joiner <- function() {
  # trans nzoia
  df_joined <- allTransNzoiaData %>%
    transmute(
      county = case_when(county %in% c("TRANS NZOIA", "Trans Nzoia") ~ "Trans Nzoia", .default = "Trans Nzoia"),
      sub_county, 
      facility_type,
      facility_name, 
      sub_category,
      keph_level, 
      tab, 
      ven = case_when(!ven %in% c('V', 'E', 'N') ~ NA, .default = ven), 
      funding,
      item_description_name_form_strength,
      pack_size = pack_size.x, price_kes = new_price_kes,
      quantity_required_for_period_specified_above,
      value_of_quantities_required_for_12_months = quantity_required_for_period_specified_above * new_price_kes,
      value_of_quantities_required_including_buffer_kes,
      value_of_quantities_to_be_procured_kes,
      contains_lab
    ) %>%
    # other counties
    bind_rows(
      allOtherCountiesData %>%
        transmute(
          county, 
          sub_county,
          facility_type,
          facility_name, 
          sub_category,
          keph_level,
          tab, 
          ven, 
          funding,
          item_description_name_form_strength,
          pack_size, price_kes, 
          quantity_required_for_period_specified_above,
          value_of_quantities_required_for_12_months = quantity_required_for_period_specified_above * price_kes,
          value_of_quantities_required_including_buffer_kes,
          value_of_quantities_to_be_procured_kes,
          contains_lab
        ) 
    ) %>%
    left_join(product_classifications %>% clean_names(), by = join_by(item_description_name_form_strength, tab)) 

  county_summary <- df_joined %>%
    filter(funding == "County") %>%
    summarise(value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE), .by = county)

  print(county_summary)
  return(df_joined)
}



library(DBI)
my_db_connection <- dbConnect(
  drv = odbc::odbc(),
  .connection_string = "Driver={PostgreSQL ANSI(x64)};",
  database = "postgres",
  UID = Sys.getenv("POSTGRES_DB_NAME"),
  PWD = Sys.getenv("POSTGRES_DB_PASSWORD"),
  Port = 5432,
  timeout = 10
)


if (!dbExistsTable(my_db_connection, "all_counties_forecast_data")) {
  # push joined data to my local postgres database server
  dbWriteTable(my_db_connection, "all_counties_forecast_data", data_joiner(), overwrite = TRUE)
  dbDisconnect(my_db_connection)
} else {
  print("Table already exists")
  dbDisconnect(my_db_connection)
}


# Table Structure ---------------------------------------------------------
# 
# postgres=# \d all_counties_forecast_data
#   Table "public.all_counties_forecast_data"
# Column                       |       Type       | Collation | Nullable | Default
# ---------------------------------------------------+------------------+-----------+----------+---------
#   county                                            | text             |           |          |
#   sub_county                                        | text             |           |          |
#   facility_name                                     | text             |           |          |
#   keph_level                                        | text             |           |          |
#   tab                                               | text             |           |          |
#   ven                                               | text             |           |          |
#   funding                                           | text             |           |          |
#   product_name                                      | text             |           |          |
#   pack_size                                         | text             |           |          |
#   price_kes                                         | double precision |           |          |
#   quantity_required_for_period_specified_above      | double precision |           |          |
#   value_of_quantities_required_for_12_months        | double precision |           |          |
#   value_of_quantities_required_including_buffer_kes | double precision |           |          |
#   value_of_quantities_to_be_procured_kes            | double precision |           |          |
#   product_or_equipment                              | text             |           |          |
#   product_classification                            | text             |           |          |
#   rmnch_product                                     | text             |           |          |
#   moh_647_name                                      | text             |           |          |
#   