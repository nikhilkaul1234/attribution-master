#install.packages("RJDBC")
#install.packages("sqldf")
#install.packages("dplyr")
#install.packages("tidyr")
#install.packages("sparklyr")
library(RJDBC)
library(sqldf)
library(tidyr)
library(dplyr)
#library(sparklyr)

sc <- spark_connect(master = "local")
spark_install()

#Establish JDBC connection to Redshift Spectrum
#substitute your credentials here and driver path below:
#driver <- JDBC("com.amazon.redshift.jdbc.Driver", "<path to Redshift .jar", identifier.quote="`")
#conn <- dbConnect(driver, "jdbc:redshift://luciskystg.gbi-lenovo.com:5439/lucicloud", "<user>", "<password>")

# Connection Info
driver <- dbDriver("PostgreSQL")
conn <-dbConnect(driver,
                 host='luciskystg.gbi-lenovo.com',
                 port='5439',
                 dbname='lucicloud',
                 user='',
                 password='')

#Pushed join of touchpoint and session data to Redshift Spectrum
#Can filter on region in the where clause.  Currently filtering data for Canada
final_tp <- dbGetQuery(conn,"select ':' || s.customer_code || coalesce(s.order_number,'') as attrib_key, 
case when tp.channel='' then 'Unknown Channel' else tp.channel end as channel,
s.customer_code,  s.amount, s.order_number,
                       case
                       when s.amount > 0 then 1 else 0 
                       end as success,
                       s.rs_id, s.country, s.state, s.product_list 
                       from 
                       (select visitor_id as customer_code, amount, order_number, session_end_time, rs_id, country, state, product_list from lenovoglobal.success where country = 'can') s
                       inner join 
                       (select visitor_id as customer_code, channel, touchpoint_time from lenovoglobal.touchpoints) tp
                       on tp.touchpoint_time <= s.session_end_time and s.customer_code = tp.customer_code")

#adding a column for channel_count
# start_data <- sqldf("select attrib_key, channel, max(success) success, count(*) as channel_count 
#                    from final_tp s group by attrib_key, channel")

start_data <- final_tp %>%
  group_by(attrib_key, channel) %>%
  summarize(success = max(success),
            channel_count = n())

#attrib-key level aggregation 
customer_data <- start_data %>% group_by(attrib_key) %>% summarise(success = max(success), totalChannels = n_distinct(channel), channel_set = paste(sort(unique(channel)), collapse = ", "))

#channel level aggregation
# channelData <- sqldf("select channel_set, totalChannels , sum(success)/count(*) as rate , sum(success) as successes , count(*) as total from customer_data group by channel_set, totalChannels")
channelData <- customer_data %>%
  group_by(channel_set, totalChannels) %>%
  summarise(rate = sum(success)/n(),
            successes = sum(success),
            total = n())

#filter down to records with only 1 channel present
oneChannelDF <- filter(channelData, totalChannels == 1)

#filter down to records with 2 channels present
twoChannelDF <- filter(channelData, totalChannels == 2)

#extract the marginal contribution for all channel combinations
marginal_df_tmp <- crossing(twoChannelDF, oneChannelDF) %>% filter(unlist(Map(function(x, y) grepl(x, y), channel_set1, channel_set)))
# marginal_df <- sqldf("select channel_set, rate-sum(rate1) as marginal from marginal_df_tmp group by rate, channel_set")
marginal_df <- marginal_df_tmp %>%
  group_by(rate, channel_set) %>%
  summarize(sum_rate1 = sum(rate1)) %>%
  mutate(marginal = rate-sum_rate1)

####### help

#create a dataframe showing single channel contribution and marginal contribution
contribution_df_tmp <- crossing(final_df, oneChannelDF) %>% filter(unlist(Map(function(x, y) grepl(x, y), channel_set1, channel_set)))
contribution_df <- sqldf("select channel_set1 as channel_set, rate, sum(marginal) as contribution from contribution_df_tmp group by channel_set1, rate")

#distinct number of channels present in the data
distinctChannelsCount = as.double(summarise(start_data, channel_count = n_distinct(channel))[1,1])
k_inverse = 1.0 / ( 2 * (distinctChannelsCount-1))

#calculate unnormalized Shapley values 
Shapley <- mutate(contribution_df, unnormalizedValues = rate + contribution*k_inverse) %>% select(channel_set, unnormalizedValues)

#calculate sum of unnormalized Shapley values for normalization in next step
total = as.double(summarise(Shapley, sum = sum(unnormalizedValues))[1,1])

#calculate normalized Shapley values
normalizedShapley <- mutate(Shapley, normalizedShapley = unnormalizedValues/total) %>% select(channel_set, normalizedShapley)

