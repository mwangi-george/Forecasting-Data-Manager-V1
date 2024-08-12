# Database Connector
lite_conn <- \(county_name, db_name = "SCDatabase"){
  con <- RSQLite::dbConnect(SQLite(), glue::glue("databases/{db_name}.db"))

  tables <- con %>%
    dbListTables() %>%
    as_tibble() %>%
    filter(value %>% str_ends(county_name)) %>%
    pull(value)
  my_db <- list(con, tables)
  return(my_db)
}


# Define County of interest
county_of_interest <- \(county_name){
  county_name %>% str_to_lower()
}


my_db_connection <- function(db_name) {
  con <- DBI::dbConnect(
    drv = odbc::odbc(), 
    .connection_string = "Driver={PostgreSQL ANSI(x64)};",
    database = db_name, 
    UID = Sys.getenv("POSTGRES_DB_NAME"),
    PWD = Sys.getenv("POSTGRES_DB_PASSWORD"), 
    Port = 5432, 
    timeout = 10
  )
  
  return(con)
}
