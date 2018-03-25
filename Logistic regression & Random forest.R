library(dplyr)
library(randomForest)
setwd("~/Desktop")
train=read.csv("model_variable_train.csv")
test=read.csv("model_variable_test.csv")

### Logistic Regression and Random Forest at 10%
### 1. Logistic Regression
#logistic regression: model and train prob
train_logistic.lr=glm(fraud ~ .,
            data=train,family =binomial)

#logistic regression:train data FDR
train_10.lr=train %>%
  mutate(train.probs.lr=predict(train_logistic.lr,train,type="response")) %>%
  arrange(desc(train.probs.lr)) %>%
  slice(1:round(0.1*nrow(train)))

train_percent.lr=
  sum(train_10.lr$fraud)/sum(train$fraud)

#logistic regression:test data FDR
test_10.lr=test %>%
  mutate(test.probs.lr=predict(train_logistic.lr,test,type="response")) %>%
  arrange(desc(test.probs.lr)) %>%
  slice(1:round(0.1*nrow(test)))

test_percent.lr=
  sum(test_10.lr$fraud)/sum(test$fraud)

## 2. Random Forest
#random forest:model and train prob
train_rf=randomForest(fraud ~ .,
                    data=train,
                    ntree = 50)

#random forest:train data FDR
train_10.rf=train %>%
  mutate(train.probs.rf=
           predict(train_rf,train,type="response")) %>%
  arrange(desc(train.probs.rf)) %>%
  slice(1:round(0.1*nrow(train)))

train_percent.rf=
  sum(train_10.rf$fraud)/sum(train$fraud)

#random forest: test data FDR
test_10.rf=test %>%
  mutate(test.probs.rf=
           predict(train_rf,test,type="response")) %>%
  arrange(desc(test.probs.rf)) %>%
  slice(1:round(0.1*nrow(test)))

test_percent.rf=
  sum(test_10.rf$fraud)/sum(test$fraud)
