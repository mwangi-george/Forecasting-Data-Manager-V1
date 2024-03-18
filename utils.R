
# Database Connector
lite_conn <- \(db_name = "SCDatabase"){
  RSQLite::dbConnect(SQLite(), glue::glue("databases/{db_name}.db"))
}


# Define County of interest
county_of_interest <- \(county_name){
  county_name %>% str_to_lower()
}
