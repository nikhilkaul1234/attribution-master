# Mapping of Attribution (Last Touch) to Investment (JP)

# Load some basic libraries
library(tidyverse)

# Connection for media spend JP Investment data
library(readxl)   

read_excel_allsheets <- function(filename, tibble = FALSE) {
  sheets <- readxl::excel_sheets(filename)
  x <- lapply(sheets, function(X) readxl::read_excel(filename, sheet = X))
  if(!tibble) x <- lapply(x, as.data.frame)
  names(x) <- sheets
  x
}

jp_sheets <- read_excel_allsheets("data/jp_dg_spend.xlsx")

jp_sheets <- lapply(jp_sheets, function(X) X[,c(4,6)])

jp <- do.call('rbind', jp_sheets)

jp$program <- rep(names(jp_sheets), each = 111)
jp$`Week Start Date` <- as.Date(jp$`Week Start Date`)

jp$program <- gsub(".*Lenovo ", "", jp$program)
jp$program <- gsub(".*NEC ", "", jp$program)

jp$program <- ifelse(jp$program == 'email', 'Email', jp$program)

# Connection for Last Touch Attribution JP

jp_atb <- read_csv('2018-05-01_jp_lasttouch_atb.csv')
jp_atb$start_date <- as.Date(jp_atb$start_date)

jp_atb <- jp_atb %>% mutate(adjusted_start_date = start_date+1)

jp <- jp %>% mutate(adjusted_start_date = ifelse(
  !(`Week Start Date` %in% unique(jp_atb$adjusted_start_date)),
  `Week Start Date` - 7,
  `Week Start Date`))

jp$adjusted_start_date <- as.Date(jp$adjusted_start_date, origin = "1970-01-01")

jp_map <- jp %>% 
  left_join(jp_atb, by = c('adjusted_start_date', 'program')) %>%
  filter(adjusted_start_date >= min(jp_atb$adjusted_start_date) &
           adjusted_start_date <= max(jp_atb$adjusted_start_date)) 

jp_map <- jp_map %>% group_by(adjusted_start_date, program) %>%
  summarise(Spend = sum(Spend),
            total_conversion = first(total_conversion),
            total_conversion_value = first(total_conversion_value))

write_csv(jp_map, 'jp_map.csv')
