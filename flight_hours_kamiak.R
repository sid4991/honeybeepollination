library(tidyverse)
library(lubridate)
library(furrr)
library(agclimtools)
library(agclimdata)


flight_hours <- function(file_path){
  
  print(file_path)

  path_split <- str_split(file_path, pattern = "/", simplify = TRUE)
  
  name_split <- str_split(path_split[,3], pattern = "_", simplify = TRUE)

  lat = as.numeric(name_split[,2])
  lon = as.numeric(name_split[,3])
  
  df <- read_binary(paste0("/data/adam/data/metdata/maca_v2_vic_binary/", file_path), num_vars = 4) |>
    mutate(doy = yday(date),
           month = month(date)) |>
    filter(month %in% c(11, 12, 1)) |> 
    add_sunrise_sunset(lat = lat) 
  df |>
    expand_grid(hour = seq(ceiling(min(df$sunrise)), floor(max(df$sunset)))) |>
    mutate(temp = temp_after_sunrise(hour, tmin, tmax, sunrise, sunset)) |>
    group_by(month, date) |> 
    summarise(flight_hours = sum(hour > sunrise & hour < sunset &
                                   between(temp, 12, 43.33) &
                                   precip < 5 &
                                   windspeed < 8.94),
              .groups = "drop") |> 
    mutate(year = year(date),
           year = if_else(month > 6, year + 1, year),
           period = case_when(between(year, 1975, 2005) ~ "Historical",
                              between(year, 2040-15, 2040+15) ~ "2040s",
                              between(year, 2060-15, 2060+15) ~ "2060s",
                              between(year, 2080-15, 2080+15) ~ "2080s")) |> 
    filter(!is.na(period)) |> 
    group_by(period, year) |> 
    summarise(flight_hours = sum(flight_hours), .groups = "drop") |> 
    mutate(lat = lat,
           lon = lon,
           model = path_split[, 1],
           climate_proj = path_split[, 2])
  
}

plan(multicore)

models <- list.dirs("/data/adam/data/metdata/maca_v2_vic_binary/", full.names = FALSE, recursive = FALSE)

available_locations <- list.files("/data/adam/data/metdata/maca_v2_vic_binary/bcc-csm1-1/historical/")

file_name <- grid_stats |> 
  filter(state %in% c("WA", "OR", "ID"), 
         location %in% available_locations) |> 
  pull(location)

climate_proj <- c("historical", "rcp45", "rcp85")

file_path <- expand_grid(models, climate_proj, file_name) |>
  mutate(file_path = paste(models, climate_proj, file_name, sep = "/")) |>
  filter(file.exists(paste0("/data/adam/data/metdata/maca_v2_vic_binary/", file_path))) |> 
  pull(file_path)

df <- future_map_dfr(file_path, flight_hours)
df_out <- df |>
  group_by(lat, lon, climate_proj, period) |>
  summarise(med_flight_hours = median(flight_hours))

write_csv(df_out, "~/honeybee/data/winter_flight_hours.csv")


