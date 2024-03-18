source("global.R")
source("utils.R")

# drive_download(
#   file = "https://docs.google.com/spreadsheets/d/1HqDmZNLubTW0opEEu2W2f9pAwx2MkFsr/edit#gid=36781670",
#   path = "data/updated_classifications.xlsx",
#   overwrite = TRUE
# )


classified_df <- read.xlsx("data/updated_classifications.xlsx")
county <- county_of_interest("isiolo")

get_data_from_db <- \(){
  lite_conn(county_name = county)[[2]] %>%
    map_df(
      .,
      ~ dbGetQuery(conn = lite_conn(county)[[1]], glue("SELECT * FROM {.x}"))
    ) -> db_output
  
  return(db_output)
}



# top 10 product categories
top_10_product_categories <- \(){
  df <- get_data_from_db() %>% 
    filter(funding == "County") %>% 
    left_join(
      classified_df %>% select(-product_or_equipment, -rmnch_product), by = join_by(item_description_name_form_strength, tab)
    ) %>% 
    summarise(
      value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE),
      .by = product_classification
    ) %>% 
    arrange(desc(value_of_quantities_required_for_12_months)) %>% 
    filter(!is.na(product_classification))
  
  library(highcharter)
  df %>% 
    slice_head(n = 10) %>% 
    hchart(
      type = "bar",
      hcaes(
        x = product_classification,
        y = value_of_quantities_required_for_12_months
      ),
      dataLabels = list(enabled = TRUE, format = "{point.y:,.0f}")
    ) %>% 
    hc_colors(colors = "lightsalmon") %>% 
    hc_title(text = "Top 10 Product Categories by Value", align = "left") %>% 
    hc_xAxis(title = "") %>% 
    hc_yAxis(title = list(text = "Value of Quantities Required for 1 Year"))
}


# top 10 product categories by level of care
top_10_product_categories_by_loc <- \(){
  df <- get_data_from_db() %>%
    filter(funding == "County") %>%
    left_join(
      classified_df %>% select(-product_or_equipment, -rmnch_product),
      by = join_by(item_description_name_form_strength, tab)
    ) %>%
    summarise(
      value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE),
      .by = c(product_classification, keph_level)
    ) %>%
    arrange(desc(value_of_quantities_required_for_12_months)) %>%
    filter(!is.na(product_classification)) %>%
    arrange(keph_level) %>%
    pivot_wider(names_from = keph_level, values_from = value_of_quantities_required_for_12_months) %>%
    adorn_totals(where = "col") %>%
    arrange(desc(Total)) %>%
    slice_head(n = 10) 
  
  df %>% 
    gt() %>%
    tab_header(
      title = "Accuracy Metrics",
      subtitle = "The lower the MAPE the better."
    ) %>%
    tab_options(
      table.width = pct(100),
      heading.background.color = "#c30010"
    ) %>% 
    opt_stylize(style = 4, color = "cyan") %>% 
    fmt_number(columns = 2:last_col(), decimals = 0)
}


# top 10 product categories by VEN
top_10_product_categories_by_ven <- \(){
  countyOfInterestForecastsWithVen <- get_data_from_db() %>% 
    filter(!is.na(ven))
  
  countyOfInterestForecastsWithoutVen <- get_data_from_db() %>% 
    filter(is.na(ven))
  
  newProductCategorization <- read.xlsx("data/newProductCategorization.xlsx") %>% 
    rename(item_description_name_form_strength = item_name, new_ven = ven, tab = product_category)
  
  ven_df <- countyOfInterestForecastsWithoutVen %>% 
    left_join(newProductCategorization, by = join_by(item_description_name_form_strength, tab)) %>% 
    mutate(ven = new_ven) %>% 
    select(-c(new_ven, product_or_equipment)) %>% 
    bind_rows(countyOfInterestForecastsWithVen) 
  
  ven_df %>% 
    filter(funding == "County") %>%
    left_join(
      classified_df %>% select(-product_or_equipment, -rmnch_product),
      by = join_by(item_description_name_form_strength, tab)
    ) %>%
    summarise(
      value_of_quantities_required_for_12_months = sum(value_of_quantities_required_for_12_months, na.rm = TRUE),
      .by = c(product_classification, ven)
    ) %>%
    arrange(desc(value_of_quantities_required_for_12_months)) %>%
    filter(!is.na(product_classification)) %>%
    arrange(ven) %>%
    pivot_wider(names_from = ven, values_from = value_of_quantities_required_for_12_months, values_fill = 0) %>%
    adorn_totals(where = "col") %>%
    arrange(desc(Total)) %>%
    slice_head(n = 10) %>%
    relocate(V, .before = E) %>% 
    rename("Product Category" = product_classification) %>%
    gt() %>%
    tab_header(
      title = "",
      subtitle = "Top 10 product categories by level of VEN"
    ) %>%
    tab_options(
      table.width = pct(100),
      heading.background.color = "#c30010"
    ) %>%
    opt_stylize(style = 4, color = "cyan") %>% 
    fmt_number(columns = 2:last_col(), decimals = 0)
}

