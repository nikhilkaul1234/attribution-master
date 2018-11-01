# Markov Attribution - Syntasa data
# Contact: mgentry@lenovo.com

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
                 user='',
                 password='')

# Countries to iterate over
countries <- c("'usa'")

i = "'usa'"
j = "'week33'"

# For now, let's comment out the loop that goes through countries and just produce an outcome for USA

#for(i in countries) {

  # Normally, we would iterate over each week, but since we have one week, let's comment out the loop
  #for (j in unique(df$week)) {

  # Pull the Markov data from Syntasa
  query <- dbSendQuery(conn, paste("select * FROM lenovoglobal.markov_data where country = ", i, "and week = ", j, sep = ""))
  df <- as.data.frame(fetch(query, n = -1))
  dbClearResult(dbListResults(conn)[[1]])
  
  # If needed, these queries will provide lists of unqqiue campaigns and programs
  # query <- dbSendQuery(conn, "select distinct(campaign_program) FROM lenovoglobal.events_of_interest")
  # list_programs <- as.data.frame(fetch(query, n = -1))
  # dbClearResult(dbListResults(conn)[[1]])
  
  # query <- dbSendQuery(conn, "select distinct(campaign_type) FROM lenovoglobal.events_of_interest")
  # list_campaigns <- as.data.frame(fetch(query, n = -1))
  # dbClearResult(dbListResults(conn)[[1]])
  
  # This does some cleanup and adds more columns for Markov
  df <- df %>%
    mutate(product_string = substring(product_string, 2)) %>% # gets the first product model number in the product_string
    mutate(first_product_model = sub("(.*?) *;.*", "\\1", product_string)) %>% # assigns the first product to a column
    mutate(full_path = paste(program_path, is_new_visitor, sep = '#')) %>% # collapses the program path and the new visitor column into one path for modeling
    mutate(Email = ifelse(grepl('Email',df$program_path), 1, 0), # the following dummies are for programs and branded status that appear in the program path
           Affiliate = ifelse(grepl('Affiliate',df$program_path), 1, 0),
           SEO = ifelse(grepl('SEO',df$program_path), 1, 0),
           Unknown_Channel = ifelse(grepl('Unknown Channel',df$program_path), 1, 0),
           CSE = ifelse(grepl('cse',df$program_path), 1, 0),
           Direct = ifelse(grepl('Direct',df$program_path), 1, 0),
           Lenovo_Social = ifelse(grepl('Lenovo Social',df$program_path), 1, 0),
           Affinity = ifelse(grepl('Affinity',df$program_path), 1, 0),
           Paid_Social = ifelse(grepl('Paid Social',df$program_path), 1, 0),
           SEM = ifelse(grepl('SEM',df$program_path), 1, 0),
           Apps = ifelse(grepl('Apps',df$program_path), 1, 0),
           Display = ifelse(grepl('Display',df$program_path), 1, 0)
           # Will need more flags or columns to back out of this
           )
  
  # Now, we prep the data frame by collapsing at the modeling path level (the full path)
  dfg <- df %>%
    mutate(null_hits = ifelse(df$success == 0, 1, 0)) %>%
    group_by(week, full_path) %>%
    summarize(revenue = sum(amount),
              quantity = sum(quantity),
              count_of_order = sum(success),
              null_hits = sum(null_hits))
  
  ## Data manipulation and formatting for markov_model command
  
  # Remove ending ">" and white spaces in between compound strings
  # Remove "_" delimiter from program names
  dfg$full_path <- gsub(" ","",dfg$full_path)
  dfg$full_path <- gsub("_","",dfg$full_path)
  
  # Add spaces between each program and ">"
  dfg$full_path <- gsub(">"," > ",dfg$full_path)
  
  # Order data by descending revenue
  dfg <- dfg %>% arrange(desc(revenue))
      
  # Processing and creation of transformed dataframe
  M <- markov_model(Data = dfg, var_path = 'full_path',
                        var_conv = 'count_of_order',
                        var_value ='revenue', 
                        var_null = 'null_hits', 
                        order = 4, max_step = 10) # order 4
    #}
  
  ## Write M to file
  write_csv(M, paste(Sys.Date(), '_', substr(i,2,3), '_markov_atb_syn_full_path.csv', sep=''))
  
#}
  
  