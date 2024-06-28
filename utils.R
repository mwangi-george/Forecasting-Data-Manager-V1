
# Database Connector
lite_conn <- \(county_name, db_name = "SCDatabase"){
  con <- RSQLite::dbConnect(SQLite(), glue::glue("databases/{db_name}.db"))
  
  tables <- con %>% 
    dbListTables() %>%
    as_tibble() %>% 
    filter(value %>% str_ends(county_name)) %>% pull(value) 
  my_db <- list(con, tables)
  return(my_db)
}


# Define County of interest
county_of_interest <- \(county_name){
  county_name %>% str_to_lower()
}

