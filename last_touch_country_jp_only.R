# Last Touch Attribution - Geo level, Country Roll-Up
# Contact: mgentry@lenovo.com

# download Amazon Redshift JDBC driver
# download.file('http://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.9.1009.jar','RedshiftJDBC41-1.1.9.1009.jar')

# install.packages("RPostgreSQL")
library(RPostgreSQL)
library(readxl)
library(xlsx)
library(readr)
library(tidyverse)
library(ChannelAttribution)

# Connection Info
drv <- dbDriver("PostgreSQL")
conn <-dbConnect(drv,
                 host='luciskystg.gbi-lenovo.com',
                 port='5439',
                 dbname='lucicloud',
                 user='',
                 password='')

# Countries to iterate over
countries <- c("'jp'")


# Main Loop: includes query, data clean-up, attribution, and write-to-file
for(i in countries) {
  query <- dbSendQuery(conn, paste("select * FROM attribution_20180402_jp where country = ", i, sep = ""))
  df <- as.data.frame(fetch(query, n = -1))
  dbClearResult(dbListResults(conn)[[1]])
  
  ## Data manipulation and formatting for markov_model command
  
  # Remove ending ">" and white spaces in between compound strings
  # Remove "_" delimiter from program names
  df$program <- gsub(" ","",df$program)
  df$program <- gsub("_","",df$program)
  
  # Add spaces between each program and ">"
  df$program <- gsub(">"," > ",df$program)
  
  # Order data by descending revenue
  df <- df %>% arrange(desc(revenue))
  
  # Processing and creation of transformed dataframe
  df$program <- sub('.*\\ ', '', df$program)
  df_g <- df %>%
    group_by(week_cycle, start_date, program) %>%
    summarize(
      total_conversion = sum(count_of_order),
      total_conversion_value = sum(revenue)
    )
  
  ## Write result to file
  write_csv(df_g, paste(Sys.Date(), '_', substr(i,2,3), '_lasttouch_atb.csv', sep=''))
  
}
