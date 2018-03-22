
library(dplyr)
library(ggplot2)
library(stringr)
library(lubridate)

setwd("~/Desktop/DSO 562/Data")


applications <- read.csv("applications.csv")



#Expert variable creation
#Before building the expert variables, lets build some special variables first
str(applications)

#Lets create a new unique variable called "person" 
# "person"  is a concatenation of the following variables: firstname, lastname, dob, ssn

applications$person = paste(applications$firstname, applications$lastname,applications$dob, applications$ssn, sep = "_")

#Lets create a new unique variable called "address_homephone"
# "fulladdress is a concatenation of the following variables: address, homephone

applications$address_homephone <- paste( applications$address, applications$homephone, sep = "_")


head(applications)

#EXPERT VARIABLES
#The project contains a variable called " date" which means that our analysis needs to involve time
# before building the expert variables, we need to set up different timeblock or time windows
# to involve time. 
# We decide to pick with 3 days and 7 days window. ( 7 days for one week worth of information) 
# This timeblock is necessary in order to capture fraudulent applications 

# Expert variables 

# expert variables
# The total count number of one entity from one variable associated/related  
# with a particular entity from another variable 
# in a particular time block in this case for 3 days and 7 days window. 
# those variables are dependent on the time.


write.csv(applications, file = "applications_edit.csv")
head(data)
write.csv(data, file = "applications_edit2.csv")

# We also create another variable called day for the time window analysis. 
# day is just the day of the variable "date"

applications1 <- read.csv("applications_edit.csv")


# phone_person, the number of person associated/related  with one particurlar phone number
# in a 3 days window time period and/or 7 days window time period

# ssn_person

# address_person

#person_zip5




head(applications)


applications1 %>%
  group_by(person) %>%
  summarise(total = n()) %>%
  arrange(desc(total)) %>%
  slice(1:10)

library(RSQLite)
library(gsubfn)
library(proto)
library(sqldf)



dd <- transform(applications)
#convdrting the right date 
dd$date <- strptime(dd$date, '%m/%d/%y')
dd$date <- as.Date(dd$date)


#just testing some sql queries 
sqldf("select a.*, count(*) ssn_person
      from dd a 
      left join dd b on b.date between a.date - 30 and a.date 
      and b.record = a.record
      and b.rowid <= a.rowid
      group by a.rowid")

library(tidyr)
library(tidyverse)

#transforming the date
dd$date <- as.Date(dd$date)

#distinct count of ssn within a three day period

dd <- dd %>% group_by(record) %>% 
  mutate(distinct_3  = map_int(date, ~n_distinct(homephone[between(date, .x - 3, .x)])), 
         distinct_7 = map_int(date, ~n_distinct(homephone[between(date, .x - 7, .x)])))


d <- dd %>% group_by(lastname) %>%
  mutate(distinct_3  = map_int(date, ~n_distinct(homephone[between(date, .x - 3, .x)])), 
         distinct_7 = map_int(date, ~n_distinct(homephone[between(date, .x - 7, .x)])))





library(data.table)
setkey(dd, date)

library(RSQLite)


#adding another special variables called fullname_birthday ( concatenation of firstname, lastname, and dob)

dd$fullname_birthday <- paste(applications$firstname, applications$lastname,applications$dob, sep = "_")

#1) EXPERT VARIABLES by looking at ssn over a window day period (3,7,14,30) ( total of 20 expert variables)

#person_ssn3 is the number of people with the same ssn in a 3-day time window 
##In other words, the number of people associated with one particular ssn over a 3 day period
##i.e. 

#person_ssn7 is the number of people with the same ssn in a 7-day time window 
#person_ssn14 is the number of people with the same ssn in a 14-day time window

#ssn_distinct_address3 is the number of people with the same ssn and different addresses over a 3 day period
# or the number of distinct addresss associated with one particular ssn over a 3 day period.
#i.e. For Record 12502, 21 people have the SSN belonging to this record with distinct (different) addresses, over a 3 day period 

# ssn_distinct_phone3 = the number of distinct phone number associated with one particular ssn over a 3 day period

# ssn_distinct_fullname_birthday3 = the number of distinct fullname_birthday (concatenation of firstname, lastname, and dob) associated with one particular ssn over a 3 day period

#  ssn_distinct_zip3 = the number of distinct zip5 associated with one particular ssn over a 3 day period


ssn_data <- sqldf('SELECT a.record,
            COUNT( CASE WHEN a.date - b.date between 0 and 2 THEN a.record ELSE NULL END) -1 AS person_ssn3,
            COUNT(CASE WHEN a.date - b.date between 0 and 6 THEN a.record ELSE NULL END) -1 AS person_ssn7,
            COUNT(CASE WHEN a.date - b.date between 0 and 13 THEN a.record ELSE NULL END) -1 AS person_ssn14,
            COUNT(CASE WHEN a.date - b.date between 0 and 29 THEN a.record ELSE NULL END) -1 AS person_ssn30,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS ssn_distinct_address3,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS ssn_distinct_address7,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS ssn_distinct_address14,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS ssn_distinct_address30,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS ssn_distinct_phone3,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS ssn_distinct_phone7,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS ssn_distinct_phone14,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS ssn_distinct_phone30,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.zip5 ELSE NULL END) -1 AS ssn_distinct_zip3,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.zip5 ELSE NULL END) -1 AS ssn_distinct_zip7,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.zip5 ELSE NULL END) -1 AS ssn_distinct_zip14,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.zip5 ELSE NULL END) -1 AS ssn_distinct_zip30,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.fullname_birthday ELSE NULL END) -1 AS ssn_distinct_fullname_birthday3,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.fullname_birthday ELSE NULL END) -1 AS ssn_distinct_fullname_birthday7,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.fullname_birthday ELSE NULL END) -1 AS ssn_distinct_fullname_birthday14,
            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.fullname_birthday ELSE NULL END) -1 AS ssn_distinct_fullname_birthday30
  FROM dd a, dd b
  WHERE a.date - b.date BETWEEN 0 AND 29 
  AND a.ssn = b.ssn
  GROUP BY 1') 
           
sum(ssn_data$person_ssn3) 
#Equal to 19,107. This means that for these 19,107 people who submitted applications, 
#in some 3-day time window, they had matching SSN with other person(s)

#just looking at the sum
sum(ssn_data$person_ssn3)  
sum(ssn_data$person_ssn7) 
sum(ssn_data$person_ssn14)
sum(ssn_data$ssn_distinct_address3)
sum(ssn_data$ssn_distinct_fullname_birthday3)
sum(ssn_data$ssn_distinct_phone14)


# 2) EXPERT VARIABLES by looking at homephone over a window day period(3,7,14,30) ( total of 20 expert variables)

#person_homephone3 = the number of person associated with one particular homephone over a 3 day period
#homephone_distinct_address3 = tthe number of distinct address associated with one particular homephone over a 3 day period
#homephone_distinct_ssn3 = the number of distinct ssn associated with one particular homephone over a 3 day period
#homephone_distinct_zip3 = the number of distinct zip5 associated with one particular homephone over a 3 day period
#homephone_distinct_fullname_birthday3 = the number of distinct fullname_birthday associated with one particular homephone over a 3 day period


homephone_data <- sqldf('SELECT a.record,
                  COUNT(CASE WHEN a.date - b.date between 0 and 2 THEN a.record ELSE NULL END) -1 AS person_homephone3,
                  COUNT(CASE WHEN a.date - b.date between 0 and 6 THEN a.record ELSE NULL END) -1 AS person_homephone7,
                  COUNT(CASE WHEN a.date - b.date between 0 and 13 THEN a.record ELSE NULL END) -1 AS person_homephone14,
                  COUNT(CASE WHEN a.date - b.date between 0 and 29 THEN a.record ELSE NULL END) -1 AS person_homephone30,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS homephone_distinct_address3,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS homephone_distinct_address7,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS homephone_distinct_address14,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS homephone_distinct_address30,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.ssn ELSE NULL END) -1 AS homephone_distinct_ssn3,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.ssn ELSE NULL END) -1 AS homephone_distinct_ssn7,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.ssn ELSE NULL END) -1 AS homephone_distinct_ssn14,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.ssn ELSE NULL END) -1 AS homephone_distinct_ssn30,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.zip5 ELSE NULL END) -1 AS homephone_distinct_zip3,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.zip5 ELSE NULL END) -1 AS homephone_distinct_zip7,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.zip5 ELSE NULL END) -1 AS homephone_distinct_zip14,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.zip5 ELSE NULL END) -1 AS homephone_distinct_zip30,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.fullname_birthday ELSE NULL END) -1 AS homephone_distinct_fullname_birthday3,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.fullname_birthday ELSE NULL END) -1 AS homephone_distinct_fullname_birthday7,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.fullname_birthday ELSE NULL END) -1 AS homephone_distinct_fullname_birthday14,
                  COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.fullname_birthday ELSE NULL END) -1 AS homephone_distinct_fullname_birthday30
                  FROM dd a, dd b
                  WHERE a.date - b.date BETWEEN 0 AND 29 
                  AND a.homephone = b.homephone
                  GROUP BY 1') 


# 3) EXPERT VARIABLES by looking at person (concatenation of firstname, lastname, ssn, and dob) over a window day period (3,7,14,30)  ( total of 12 expert variables)


# person_person3 = the number of people associated with the same person ( person being a concatenation of firstname, lastname, ssn, and dob )
# person_distinct_address3 = the number of distinct address associated with the same person ( person being a concatenation of firstname, lastname, ssn, and dob )
# person_distinct_zip3 = the number of distinct zip5 associated with the same person ( person being a concatenation of firstname, lastname, ssn, and dob )

person_data <- sqldf('SELECT a.record,
                        COUNT(CASE WHEN a.date - b.date between 0 and 2 THEN a.record ELSE NULL END) -1 AS person_person3,
                        COUNT(CASE WHEN a.date - b.date between 0 and 6 THEN a.record ELSE NULL END) -1 AS person_person7,
                        COUNT(CASE WHEN a.date - b.date between 0 and 13 THEN a.record ELSE NULL END) -1 AS person_person14,
                        COUNT(CASE WHEN a.date - b.date between 0 and 29 THEN a.record ELSE NULL END) -1 AS person_person30,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS person_distinct_address3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS person_distinct_address7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS person_distinct_address14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS person_distinct_address30,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.zip5 ELSE NULL END) -1 AS person_distinct_zip3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.zip5 ELSE NULL END) -1 AS person_distinct_zip7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.zip5 ELSE NULL END) -1 AS person_distinct_zip14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.zip5 ELSE NULL END) -1 AS person_distinct_zip30
                        FROM dd a, dd b
                        WHERE a.date - b.date BETWEEN 0 AND 29 
                        AND a.person = b.person
                        GROUP BY 1') 

# 4) EXPERT VARIABLES by looking at address over a window day period (3,7,14,30 ) ( total of 20 expert variables)


# person_address3 = the number of person associated with one particular address over a 3 day period
# address_distinct_homephone3 = the number of distinct homephone associated with one particular address over a 3 day period
# address_distinct_ssn3 = the number of distinct ssn associated with one particular address over a 3 day period
# address_distinct_zip3 = the number of distinct zip5 associated with one particular address over a 3 day period
# address_distinct_fullname_birthday3 = the number of distinct fullname_birthday (concatenation of firstname, lastname, and dob) associated with one particular address over a 3 day period



address_data <- sqldf('SELECT a.record,
                        COUNT(CASE WHEN a.date - b.date between 0 and 2 THEN a.record ELSE NULL END) -1 AS person_address3,
                        COUNT(CASE WHEN a.date - b.date between 0 and 6 THEN a.record ELSE NULL END) -1 AS person_address7,
                        COUNT(CASE WHEN a.date - b.date between 0 and 13 THEN a.record ELSE NULL END) -1 AS person_address14,
                        COUNT(CASE WHEN a.date - b.date between 0 and 29 THEN a.record ELSE NULL END) -1 AS person_address30,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS address_distinct_homephone3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS address_distinct_homephone7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS address_distinct_homephone14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS address_distinct_homephone30,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.ssn ELSE NULL END) -1 AS address_distinct_ssn3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.ssn ELSE NULL END) -1 AS address_distinct_ssn7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.ssn ELSE NULL END) -1 AS address_distinct_ssn14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.ssn ELSE NULL END) -1 AS address_distinct_ssn30,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.zip5 ELSE NULL END) -1 AS address_distinct_zip3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.zip5 ELSE NULL END) -1 AS address_distinct_zip7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.zip5 ELSE NULL END) -1 AS address_distinct_zip14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.zip5 ELSE NULL END) -1 AS address_distinct_zip30,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.fullname_birthday ELSE NULL END) -1 AS address_distinct_fullname_birthday3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.fullname_birthday ELSE NULL END) -1 AS address_distinct_fullname_birthday7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.fullname_birthday ELSE NULL END) -1 AS address_distinct_fullname_birthday14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.fullname_birthday ELSE NULL END) -1 AS address_distinct_fullname_birthday30
                        FROM dd a, dd b
                        WHERE a.date - b.date BETWEEN 0 AND 29 
                        AND a.address = b.address
                        GROUP BY 1') 

# 5) EXPERT VARIABLES by looking at zip5 over a window day period (3,7,14,30 ) ( total of 20 expert variables)

# person_zip3 = the number of person associated with one particular zip code over a 3 day period
# zip_distinct_homephone3 = the number of distinct homephone associated with one particular zip code over a 3 day period
# zip_distinct_ssn3 = the number of distinct ssn associated with one particular zip code over a 3 day period
# zip_distinct_address3 = the number of distinct address associated with one particular zip code over a 3 day period
# zip_distinct_fullname_birthday3 = the number of distinct fullname_birthday (concatenation of firstname, lastname, and dob) associated with one particular zip code over a 3 day period



zip5_data <- sqldf('SELECT a.record,
                      COUNT(CASE WHEN a.date - b.date between 0 and 2 THEN a.record ELSE NULL END) -1 AS person_zip3,
                      COUNT(CASE WHEN a.date - b.date between 0 and 6 THEN a.record ELSE NULL END) -1 AS person_zip7,
                      COUNT(CASE WHEN a.date - b.date between 0 and 13 THEN a.record ELSE NULL END) -1 AS person_zip14,
                      COUNT(CASE WHEN a.date - b.date between 0 and 29 THEN a.record ELSE NULL END) -1 AS person_zip30,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS zip_distinct_homephone3,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS zip_distinct_homephone7,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS zip_distinct_homephone14,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS zip_distinct_homephone30,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.ssn ELSE NULL END) -1 AS zip_distinct_ssn3,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.ssn ELSE NULL END) -1 AS zip_distinct_ssn7,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.ssn ELSE NULL END) -1 AS zip_distinct_ssn14,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.ssn ELSE NULL END) -1 AS zip_distinct_ssn30,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS zip_distinct_address3,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS zip_distinct_address7,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS zip_distinct_address14,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS zip_distinct_address30,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.fullname_birthday ELSE NULL END) -1 AS zip_distinct_fullname_birthday3,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.fullname_birthday ELSE NULL END) -1 AS zip_distinct_fullname_birthday7,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.fullname_birthday ELSE NULL END) -1 AS zip_distinct_fullname_birthday14,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.fullname_birthday ELSE NULL END) -1 AS zip_distinct_fullname_birthday30
                      FROM dd a, dd b
                      WHERE a.date - b.date BETWEEN 0 AND 29 
                      AND a.zip5 = b.zip5
                      GROUP BY 1') 

# 6) EXPERT VARIABLES by looking at dob over a window day period (3,7,14,30 ) ( total of 16 expert variables)

# person_dob3 = the number of person associated with one particular dob ( date of birth) code over a 3 day period
# dob_distinct_homephone3 = the number of distinct homephone associated with one particular don over a 3 day period
# dob_distinct_ssn3 = the number of distinct ssn associated with one particular dob over a 3 day period
# dob_distinct_address3 = the number of distinct address associated with one particular dob over a 3 day period



dob_data <- sqldf('SELECT a.record,
                   COUNT(CASE WHEN a.date - b.date between 0 and 2 THEN a.record ELSE NULL END) -1 AS person_dob3,
                   COUNT(CASE WHEN a.date - b.date between 0 and 6 THEN a.record ELSE NULL END) -1 AS person_dob7,
                   COUNT(CASE WHEN a.date - b.date between 0 and 13 THEN a.record ELSE NULL END) -1 AS person_dob14,
                   COUNT(CASE WHEN a.date - b.date between 0 and 29 THEN a.record ELSE NULL END) -1 AS person_dob30,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS dob_distinct_homephone3,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS dob_distinct_homephone7,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS dob_distinct_homephone14,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS dob_distinct_homephone30,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.ssn ELSE NULL END) -1 AS dob_distinct_ssn3,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.ssn ELSE NULL END) -1 AS dob_distinct_ssn7,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.ssn ELSE NULL END) -1 AS dob_distinct_ssn14,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.ssn ELSE NULL END) -1 AS dob_distinct_ssn30,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS dob_distinct_address3,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS dob_distinct_address7,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS dob_distinct_address14,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS dob_distinct_address30
                   FROM dd a, dd b
                   WHERE a.date - b.date BETWEEN 0 AND 29 
                   AND a.dob = b.dob
                   GROUP BY 1') 


# Now merging all our expert variables into one 

?merge


expertdata <- cbind(address_data, dob_data, homephone_data, person_data, ssn_data, zip5_data)

dim(expertdata)

write.csv(expertdata, 'expertdata.csv')

#using excel to clean duplicate columns of "record"

# now reloading again into the r script

expertdata <- read.csv("expertdata.csv")

dim(expertdata)

expertdata <- expertdata[,-1]
dim(expertdata)
