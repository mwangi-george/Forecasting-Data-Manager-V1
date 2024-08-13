
# Packages ----------
pacman::p_load(tidyverse, DBI, httr, memoise, janitor, glue, memoise)
source("utils.R")

county_name <- readline(prompt = "Enter name of this County, e.g Nairobi: ")
county_id <- readline(prompt = "Enter id for this county e.g jkG3zaihdSs: ")

# Edit here
county_of_interest_id <- c(county_name = county_id)

# define dynamic path to save data
path_to_save <- glue("data/Workload_by_County/{county_name}.xlsx")


# API Definition ------------
api_definition <- function(data_period, county_id) {
  base_url <- "https://hiskenya.org/api/analytics.csv?"
  
  data_elements <- "dimension=dx%3AuTQOSp7rx5h.wBWcFk7k1qY%3BuTQOSp7rx5h.K4WLOEhtcvC%3BDx3XYket0sf.wBWcFk7k1qY%3BDx3XYket0sf.K4WLOEhtcvC%3BEsfvxFcltV0.wBWcFk7k1qY%3BEsfvxFcltV0.K4WLOEhtcvC%3BJQkE0bN9OTB.wBWcFk7k1qY%3BJQkE0bN9OTB.K4WLOEhtcvC%3Bd3jNYZueSmn.wBWcFk7k1qY%3Bd3jNYZueSmn.K4WLOEhtcvC%3Baq64ukEqs2N.wBWcFk7k1qY%3Baq64ukEqs2N.K4WLOEhtcvC%3BhAH08l33p9B.wBWcFk7k1qY%3BhAH08l33p9B.K4WLOEhtcvC%3BJGbmLfjZPEf.wBWcFk7k1qY%3BJGbmLfjZPEf.K4WLOEhtcvC%3BSyUQl638r5P.wBWcFk7k1qY%3BSyUQl638r5P.K4WLOEhtcvC%3BYyKbS5UrJlQ.wBWcFk7k1qY%3BYyKbS5UrJlQ.K4WLOEhtcvC%3BHa96SBKI06u.wBWcFk7k1qY%3BHa96SBKI06u.K4WLOEhtcvC%3BF6AwnxbwiK2.wBWcFk7k1qY%3BF6AwnxbwiK2.K4WLOEhtcvC%3Bx13zGZKuxzs.wBWcFk7k1qY%3Bx13zGZKuxzs.K4WLOEhtcvC%3BzXzBILTVJzD.wBWcFk7k1qY%3BzXzBILTVJzD.K4WLOEhtcvC%3BrIkD2jIDqPh.wBWcFk7k1qY%3BrIkD2jIDqPh.K4WLOEhtcvC%3BnqysBKopEhA.wBWcFk7k1qY%3BnqysBKopEhA.K4WLOEhtcvC%3BOYSFd2DqOPN.wBWcFk7k1qY%3BOYSFd2DqOPN.K4WLOEhtcvC%3BT22hPVk4yGZ.wBWcFk7k1qY%3BT22hPVk4yGZ.K4WLOEhtcvC%3BQStt76mU06X.wBWcFk7k1qY%3BQStt76mU06X.K4WLOEhtcvC%3BaMKXyBh2GcE.wBWcFk7k1qY%3BaMKXyBh2GcE.K4WLOEhtcvC%3BoZWvITIip2r.wBWcFk7k1qY%3BoZWvITIip2r.K4WLOEhtcvC%3BDpR59HXC0jW.wBWcFk7k1qY%3BDpR59HXC0jW.K4WLOEhtcvC%3BWEW9Bjga52F.wBWcFk7k1qY%3BWEW9Bjga52F.K4WLOEhtcvC%3BuD5QMzjrHw0.wBWcFk7k1qY%3BuD5QMzjrHw0.K4WLOEhtcvC%3BPt64kYEr78h.wBWcFk7k1qY%3BPt64kYEr78h.K4WLOEhtcvC%3BzEn9tWUgR5P.wBWcFk7k1qY%3BzEn9tWUgR5P.K4WLOEhtcvC%3BixNhGQrVorC.wBWcFk7k1qY%3BixNhGQrVorC.K4WLOEhtcvC%3BpSWtrq3aY9C.wBWcFk7k1qY%3BpSWtrq3aY9C.K4WLOEhtcvC%3BIzvuj3rkju4.wBWcFk7k1qY%3BIzvuj3rkju4.K4WLOEhtcvC&"
  
  period <- "dimension=pe%3A{data_period}&"
  
  org_units <- glue("dimension=ou%3A{county_id}%3BLEVEL-t9kwHRyMyOC&")
  
  facility_ownerships <- "dimension=JlW9OiK1eR4%3AeT1vvFVhLHc%3BAaAF5EmS1fk%3BaRxa6o8GqZN&"
  
  keph_levels <- "dimension=sytI3XYbxwE%3AtvMxZ8aCVou%3Bwwiu1jyZOXO%3BhBZ5DRto7iF%3Bd5QX71PY5t0%3BFpY8vg4gh46&"
  
  other_params <- "showHierarchy=true&hierarchyMeta=true&includeMetadataDetails=true&includeNumDen=true&skipRounding=false&completedOnly=false&outputIdScheme=UID"
  
  full_url <- glue(base_url, data_elements, period, org_units, facility_ownerships, keph_levels, other_params)
  return(full_url)
}

# Download and aggregate Function -------------
download_data <- memoise(
  function(){
    if (curl::has_internet()) {
      start <- Sys.time()
      df <- GET(
        url = api_definition(2023, county_of_interest_id),
        authenticate(user = Sys.getenv("DHIS2_USERNAME"), password = Sys.getenv("DHIS2_PASSWORD")) 
      ) %>% 
        content() %>% 
        rawToChar() %>% 
        read.csv(text = .) %>% 
        clean_names() %>% 
        select(-c(numerator:divisor)) %>% 
        summarise(
          value=sum(value, na.rm = TRUE),
          .by = c(organisation_unit, facility_ownership, keph_level)
        )
      
      end <- Sys.time()
      print(end - start)
      return(df)
    } else {
      print("No Internet Connection!")
    }
  }
)


# Merge, Transform and Save Function -------------
merge_transform_save <- function(){
  
  input_df <- download_data()
  
  # Requirements - Datasets
  all_orgs <- my_db_connection("FP") %>% 
    dbGetQuery("select * from organisation_units") 
  
  keph_levels <- my_db_connection("FP") %>% 
    dbGetQuery("select * from keph_levels") 
  
  facility_ownerships <- my_db_connection("FP") %>% 
    dbGetQuery("select * from facility_ownerships") %>% select(-facility_ownership)
  
  
  # Join datasets
  output_df <- all_orgs %>% 
    right_join(input_df, by = join_by(facility_id == organisation_unit)) %>%
    inner_join(keph_levels, by = join_by(keph_level == keph_level_id)) %>% 
    inner_join(facility_ownerships, by = join_by(facility_ownership == facility_ownership_id)) %>% 
    # Select required columns 
    transmute(
      keph_level=keph_level_name,
      county=county_name,
      sub_county=sub_county_name,
      ward=ward_name,
      facility_name,
      facility_id,
      sub_county_2=sub_county_name,
      mfl_code,
      facility_ownership=facility_ownership_short,
      total_workload=value
    ) %>% 
    filter(facility_ownership == "Ministry of Health")
  
  print(output_df %>% head())
  
  # save
  output_df %>% openxlsx::write.xlsx(path_to_save)
  
  # Open saved file in Excel - get input for facility classification
  output_df %>%
    select(keph_level, sub_county, facility_name, workload = total_workload, mfl_code, facility_ownership) %>% 
    show_in_excel()
  
}


# Call defined functions
merge_transform_save()





















































