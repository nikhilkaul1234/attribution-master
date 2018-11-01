# Last Touch Attribution - Geo level, Country Roll-Up
# Contact: mgentry@lenovo.com

# download Amazon Redshift JDBC driver
# download.file('http://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.9.1009.jar','RedshiftJDBC41-1.1.9.1009.jar')

# install.packages("RPostgreSQL")
library(RPostgreSQL) # connection to redshift
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
                 user='metric1',
                 password='4exhjGpb6Eopzn1')

# Countries to iterate over
countries <- c("'us'","'ca'")



for(i in countries) {
  query <- dbSendQuery(conn, paste("select * FROM attribution_20180308 where country = ", i, sep = ""))
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
  M <- markov_model(Data = df, var_path = 'program',
                    var_conv = 'count_of_order',
                    var_value ='revenue', 
                    var_null = 'null_hits', 
                    order = 1, max_step = 10)
  
  ## Write M to file
  write_csv(M, paste(Sys.Date(), '_', substr(i,2,3), '_markov_atb.csv', sep=''))
  
}

