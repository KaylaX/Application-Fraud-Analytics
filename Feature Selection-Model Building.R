library(dplyr)
library(ggplot2)
library(stringr)
library(lubridate)
library(tidyverse)
library(ROCR)
library(rattle)
library(lattice)
library(caret)
library(RSQLite)
library(gsubfn)
library(proto)
library(sqldf)



applications$date = mdy(applications$date)
datadb = dbConnect(SQLite(), dbname = 'data.sqlite')
dbWriteTable(conn = datadb, name = "dd", applications, overwrite=T, row.names=F)


ssn_data1 <- dbGetQuery(datadb, 
                              "SELECT a.record,
                              COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_ssn_1,
                              COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_ssn_3,
                              COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_ssn_7,
                              COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_ssn_14,
                              COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_ssn_30,
                              COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_1, 
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_3,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_7,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_14,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_30,
                              COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_phone_1, 
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_homephone_3,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_homephone_7,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_homephone_14,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_homephone_30,
                              COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_1, 
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_3,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_7,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_14,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_30,
                              COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zip5_1, 
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zip5_3,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zip5_7,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zip5_14,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zip5_30
                              FROM app a, app b
                              WHERE a.date - b.date BETWEEN 0 AND 29 
                              AND a.ssn = b.ssn
                              AND a.record >= b.record
                              GROUP BY 1")


homephone_data1 <- dbGetQuery(datadb, 
                        "SELECT a.record,
                        COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_homephone_1,
                        COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_homephone_3,
                        COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_homephone_7,
                        COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_homephone_14,
                        COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_homephone_30,
                        COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_1, 
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_30,
                        COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.zip5 ELSE NULL END) -1 AS same_homephone_diff_zip5_1, 
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.ssn ELSE NULL END) -1 AS same_homephone_diff_ssn_3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.ssn ELSE NULL END) -1 AS same_homephone_diff_ssn_7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.ssn ELSE NULL END) -1 AS same_homephone_diff_ssn_14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.ssn ELSE NULL END) -1 AS same_homephone_diff_ssn_30,
                        COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_1, 
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_30,
                        COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_homephone_diff_zip5_1, 
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_homephone_diff_zip5_3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_homephone_diff_zip5_7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_homephone_diff_zip5_14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_homephone_diff_zip5_30
                        FROM app a, app b
                        WHERE a.date - b.date BETWEEN 0 AND 29 
                        AND a.homephone = b.homephone
                        AND a.record >= b.record
                        GROUP BY 1")



zip5_data1 <- dbGetQuery(datadb, 
                              "SELECT a.record,
                              COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_zip5_1,
                              COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_zip5_3,
                              COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_zip5_7,
                              COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_zip5_14,
                              COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_zip5_30,
                              COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.address ELSE NULL END) -1 AS same_zip5_diff_address_1, 
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS same_zip5_diff_address_3,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS same_zip5_diff_address_7,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS same_zip5_diff_address_14,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS same_zip5_diff_address_30,
                              COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.homephone ELSE NULL END) -1 AS same_zip5_diff_phone_1, 
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS same_zip5_diff_homephone_3,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS same_zip5_diff_homephone_7,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS same_zip5_diff_homephone_14,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS same_zip5_diff_homephone_30,
                              COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip5_diff_bdname_1, 
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip5_diff_bdname_3,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip5_diff_bdname_7,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip5_diff_bdname_14,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip5_diff_bdname_30,
                              COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip5_diff_zip5_1, 
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip5_diff_ssn_3,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip5_diff_ssn_7,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip5_diff_ssn_14,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip5_diff_ssn_30
                              FROM app a, app b
                              WHERE a.date - b.date BETWEEN 0 AND 29 
                              AND a.zip5 = b.zip5
                              AND a.record >= b.record
                              GROUP BY 1")


address_data1 <- dbGetQuery(datadb, 
                         "SELECT a.record,
                         COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_address_1,
                         COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_address_3,
                         COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_address_7,
                         COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_address_14,
                         COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_address_30,
                         COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip5_1, 
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip5_3,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip5_7,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip5_14,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip5_30,
                         COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.homephone ELSE NULL END) -1 AS same_address_diff_phone_1, 
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS same_address_diff_homephone_3,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS same_address_diff_homephone_7,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS same_address_diff_homephone_14,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS same_address_diff_homephone_30,
                         COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_1, 
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_3,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_7,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_14,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_30,
                         COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_address_diff_zip5_1, 
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_address_diff_zip5_3,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_address_diff_zip5_7,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_address_diff_zip5_14,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_address_diff_zip5_30
                         FROM app a, app b
                         WHERE a.date - b.date BETWEEN 0 AND 29 
                         AND a.address = b.address
                         AND a.record >= b.record
                         GROUP BY 1")


expertdata1 <- cbind(address_data1, homephone_data1, ssn_data1, zip5_data1)

dim(expertdata1)

write.csv(expertdata1, 'expertdata1.csv')




## training and testing


expertdata3 <- read.csv('training_testingdiff.csv')


## 80% of the sample size
smp_size <- floor(0.80 * nrow(expertdata3))

## set the seed to make your partition reproductible
set.seed(123)
train_index <- sample(seq_len(nrow(expertdata3)), size = smp_size)

train <- expertdata3[train_index, ]
test <- expertdata3[-train_index, ]

ks.test(train$same_address_1, train$fraud)
myks(train$fraud, train$same_address_1)

#KS calculation
myks<-function(y,x){ 
  glm.fit=glm(y ~ x,family=binomial,data=train)
  glm.predict=predict(glm.fit,type="response")
  pred <- prediction(predictions = glm.predict,labels=y)  
  perf <- performance(pred,"tpr","fpr")  
  tmp <- max(attr(perf,"y.values")[[1]]- attr(perf,"x.values")[[1]])  
  return(tmp)}

ks_data <- data.frame(variable=colnames(train),ks_value=apply(train,2,function(x,y)myks(train$fraud,x)))



ks_data %>%
  arrange(desc(ks_value)) %>%
  slice(1:40)

write.csv(ks_data, "ks_data3.csv")

to.keep <- c("same_ssn_30","same_address_30", "same_zip5_30", "same_ssn_14", "same_zip5_14", "same_address_14", "same_ssn_7",
             "same_address_7", "same_ssn_3", "same_zip5_7", "same_address_3", "same_zip5_3", "same_homephone_diff_zip5_7",
             "same_homephone_diff_zip5_7.1", "same_homephone_diff_bdname_7", "same_homephone_diff_address_7", "same_homephone_diff_zip5_3",
             "same_homephone_diff_zip5_3.1", "same_homephone_diff_address_3", "same_homephone_diff_bdname_3", "same_homephone_diff_zip5_1",
             "same_homephone_diff_zip5_1.1", "same_homephone_diff_address_1", "same_homephone_diff_address_1", "same_zip5_diff_homephone_30",
             "same_zip5_diff_ssn_30", "same_zip5_diff_address_30", "same_ssn_1", "same_zip5_diff_bdname_30", "same_zip5_diff_address_30", 
             "same_homephone_diff_zip5_14", "same_homephone_diff_address_14", "same_homephone_diff_ssn_14", "same_homephone_diff_bdname_14",
             "same_homephone_diff_zip5_1", "same_homephone_diff_address_1", "same_homephone_diff_bdname_1", "same_ssn_1", "same_zip5_1", "same_homephone_3"
             ,"same_homephone_7", "same_homephone_14", "same_homephone_diff_address_30", "fraud")


train <- train[,which(names(train) %in% to.keep)]

test <- test[,which(names(test) %in% to.keep)]

#backward selection 
fullmod<- glm(fraud ~.,data = train, family=binomial)
summary(fullmod)


backwards <- step(fullmod)


to.keep2 <- c('same_address_3' ,'same_address_7', 'same_address_30' , 'same_homephone_14' , 
                'same_homephone_diff_address_3' , 'same_homephone_diff_address_7' , 
                'same_homephone_diff_address_30' , 'same_homephone_diff_bdname_7' , 
                'same_homephone_diff_zip5_3' , 'same_homephone_diff_zip5_7' , 
                'same_ssn_7' , 'same_zip5_3' , 'same_zip5_7 , same_zip5_30 ', 'same_zip5_diff_address_30' , 
                'same_zip5_diff_ssn_30', 'fraud')

train2 <- train[,which(names(train) %in% to.keep2)]



test2 <- test[,which(names(test) %in% to.keep2)]


outoftime2 <- read.csv('outoftime_diff.csv')

#### logistic regression

glm.fit=glm(fraud ~ same_address_3 + same_address_7 + same_address_30 +same_homephone_14 + same_homephone_diff_address_3
            + same_homephone_diff_address_7 + same_homephone_diff_address_30 + same_homephone_diff_zip5_3 + same_homephone_diff_zip5_7
            + same_ssn_7,
            data=train2,family =binomial)

# 1) test data
glm.probs=predict(glm.fit,test2, type="response")

testdatawithprobs=cbind(test2,glm.probs)

write.csv(testdatawithprobs,"testoutlr.csv")


logistic_test <- test2%>%
  mutate(predict_value = glm.probs) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(test2)))

fdr1_test_percent <- sum(logistic_test$fraud) / sum(test2$fraud)


#2) training data

glm.probs1=predict(glm.fit,train2, type="response")

logistic_train <- train2%>%
  mutate(predict_value = glm.probs1) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(train2)))

fdr1_train_percent <- sum(logistic_train$fraud) / sum(train2$fraud)

# 3) out of time

glm.probs2=predict(glm.fit,outoftime2, type="response")

logistic_outoftime <- outoftime2%>%
  mutate(predict_value = glm.probs2) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(outoftime2)))

fdr1_outoftime_percent <- sum(logistic_outoftime$fraud) / sum(outoftime2$fraud)

####  Random forest 
library(randomForest)

rf.fit=randomForest(fraud ~ same_address_3 + same_address_7 + same_address_30 +same_homephone_14 + same_homephone_diff_address_3
                    + same_homephone_diff_address_7 + same_homephone_diff_address_30 + same_homephone_diff_zip5_3 + same_homephone_diff_zip5_7
                    + same_ssn_7,data=train2,ntree = 1000)

# 1) test data

rf.probs=predict(rf.fit,test2)

rf_test <- test2%>%
  mutate(predict_value = rf.probs) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.02 * nrow(test2)))


fdr2_test_percent <- sum(rf_test$fraud) / sum(test2$fraud)
fdr2_test_percent

# 2) training data 

rf.probs1=predict(rf.fit,train2)

rf_train <- train2%>%
  mutate(predict_value = rf.probs1) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(train2)))

fdr2_train_percent <- sum(rf_train$fraud) / sum(train2$fraud)


# 3) out of time

rf.probs2=predict(rf.fit,outoftime2, type="response")

rf_outoftime <- outoftime2%>%
  mutate(predict_value = rf.probs2) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(outoftime2)))

fdr2_outoftime_percent <- sum(rf_outoftime$fraud) / sum(outoftime2$fraud)


## SVM 

library(e1071)
library(kernlab)
library(parallelSVM)

svm_model <- parallelSVM(fraud ~ same_address_3 + same_address_7 + same_address_30 +same_homephone_14 + same_homephone_diff_address_3
                         + same_homephone_diff_address_7 + same_homephone_diff_address_30 + same_homephone_diff_zip5_3 + same_homephone_diff_zip5_7
                         + same_ssn_7 ,
                         data=train2)


#1) test 

svm.probs <- predict(svm_model, test2)

svm_test <- test2%>%
  mutate(predict_value = svm.probs) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(test2)))

fdr3_test_percent <- sum(svm_test$fraud) / sum(test2$fraud)


# 2) training 

svm.probs1=predict(svm_model,train2)

svm_train <- train2%>%
  mutate(predict_value = svm.probs1) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(train2)))

fdr3_train_percent <- sum(svm_train$fraud) / sum(train2$fraud)


# 3) Out of time

svm.probs2=predict(svm_model,outoftime2, type="response")

svm_outoftime <- outoftime2%>%
  mutate(predict_value = svm.probs2) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(outoftime2)))

fdr3_outoftime_percent <- sum(svm_outoftime$fraud) / sum(outoftime2$fraud)


## Gradient Boosting
library(MASS)
library(survival)
library(splines)
library(parallel)
library(gbm)
gbm.fit <- gbm(fraud ~ same_address_3 + same_address_7 + same_address_30 +same_homephone_14 + same_homephone_diff_address_3
               + same_homephone_diff_address_7 + same_homephone_diff_address_30 + same_homephone_diff_zip5_3 + same_homephone_diff_zip5_7
               + same_ssn_7,data = train2, distribution = "gaussian",n.trees = 100,
                  shrinkage = 0.01, interaction.depth = 4)

# 1) test

gbm.probs <- predict(gbm.fit, test2, n.trees = 100)

gbm_test <- test2%>%
  mutate(predict_value = gbm.probs) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(test2)))

fdr4_test_percent <- sum(gbm_test$fraud) / sum(test2$fraud)



# 2) training 

gbm.probs1=predict(gbm.fit,train2, n.trees = 100)

gbm_train <- train2%>%
  mutate(predict_value = gbm.probs1) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(train2)))

fdr4_train_percent <- sum(gbm_train$fraud) / sum(train2$fraud)

# 3) out of time

gbm.probs2=predict(gbm.fit,outoftime2, n.trees = 100)

gbm_outoftime <- outoftime2%>%
  mutate(predict_value = gbm.probs2) %>%
  arrange(desc(predict_value)) %>%
  slice(1:round(0.10 * nrow(outoftime2)))

fdr4_outoftime_percent <- sum(gbm_outoftime$fraud) / sum(outoftime2$fraud)


for (i in seq(from=0.01, to=0.25, by=0.01)) {
  rf_test <- test2%>%
    mutate(predict_value = rf.probs) %>%
    arrange(desc(predict_value)) %>%
    slice(1:round(i * nrow(test2)))
  
  fdr2_test_percent <- sum(rf_test$fraud) / sum(test2$fraud)
  number_of_records<- i*nrow(test2) - (i-0.01) * nrow(test2)
  number_of_good <-  sum(test2$fraud[1:round(i * nrow(test2))]) - sum(rf_test$fraud)
  
  print(i)
  print(fdr2_test_percent)
  print(number_of_records)
  print(number_of_good)
}

library(caret)

for (i in seq(from=0.01, to=0.25, by=0.01)) {
  rf_outoftime <- outoftime2%>%
    mutate(predict_value = rf.probs2) %>%
    arrange(desc(predict_value)) %>%
    slice(1:round(i * nrow(outoftime2)))
  
  fdr2_outoftime_percent <- sum(rf_outoftime$fraud) / sum(outoftime2$fraud)
  number_of_records<- i*nrow(outoftime2) - (i-0.01) * nrow(outoftime2)
  number_of_bad <-  sum(rf_outoftime$fraud[1:round(i * nrow(outoftime2))]) - sum(rf_outoftime$fraud[1:round((i-0.01) * nrow(outoftime2))])
            
  print(i)
  print(fdr2_outoftime_percent)
  print(number_of_records)
  print(number_of_bad)
}

#how many our model predicted 0 for fraud

length(rf_test$fraud) - sum(rf_test$fraud)  



#how manu goods they are in the test data   
length(outoftime2$fraud) - sum(outoftime2$fraud)

sum(outoftime2$fraud)


