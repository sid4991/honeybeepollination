library(tidyverse)
library(furrr)
library(vroom)

honeybee_path <- "/scratch/user/julien.pireaud/20210924_220144"

summarise_honeybee <- function(location){
  
  expand_grid(model = list.dirs(honeybee_path, recursive = FALSE, full.names = FALSE),
              climate_proj = list.dirs(file.path(honeybee_path, "bcc-csm1-1/"), 
                                       recursive = FALSE, full.names = FALSE),
              location = location) |> 
    mutate(data = map(file.path(honeybee_path, model, climate_proj, location), vroom)) |> 
    unnest(data) |> 
    mutate(period = case_when(between(Year, 1975, 2005) ~ "Historical",
                              between(Year, 2040 - 15, 2040 + 15) ~ "2040s",
                              between(Year, 2060 - 15, 2060 + 15) ~ "2060s",
                              between(Year, 2080 - 15, 2080 + 15) ~ "2080s")) |> 
    filter(!is.na(period)) |> 
    select(-Year) |> 
    group_by(location, climate_proj, period) |> 
    summarise(across(where(is.numeric), median))
  
}

plan(multicore)

df <- future_map_dfr(list.files(file.path(honeybee_path, "bcc-csm1-1", "rcp45")),
               summarise_honeybee)
write_csv(df, "honeybee/data/honeybee_map_summary.csv")

