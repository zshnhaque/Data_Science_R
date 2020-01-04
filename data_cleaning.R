rm(list = ls())
gc();
gc();
library(data.table)
library(e1071)

'%nin%' = Negate('%in%')
train = fread("D:/data_science/data/house-prices-advanced-regression-techniques/train.csv",sep = ",",
              colClasses = "character",na.strings = c(NA,'NULL','null','N/A','',' '))
names(train)

fact_vars = c("MSSubClass", "MSZoning",  "Street", 
              "Alley", "LotShape", "LandContour", "Utilities", "LotConfig", 
              "LandSlope", "Neighborhood", "Condition1", "Condition2", "BldgType", 
              "HouseStyle", "OverallQual", "OverallCond",
              "RoofStyle", "RoofMatl", "Exterior1st", "Exterior2nd", "MasVnrType", 
               "ExterQual", "ExterCond", "Foundation", "BsmtQual", 
              "BsmtCond", "BsmtExposure", "BsmtFinType1",  "BsmtFinType2", 
                "Heating", "HeatingQC", "Fireplaces",
              "CentralAir", "Electrical",  "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath", 
              "BedroomAbvGr", "KitchenAbvGr", "KitchenQual",
              "Functional", "FireplaceQu", "GarageType", 
              "GarageFinish","GarageQual", "GarageCond", 
              "PavedDrive",  "PoolQC", "Fence", "MiscFeature", 
                "SaleType", "SaleCondition","MoSold")
  
date_field = c( "YearBuilt", "YearRemodAdd","GarageYrBlt", "YrSold")
num_vars = c("LotFrontage","LotArea","MasVnrArea", "BsmtFinSF1","BsmtFinSF2","BsmtUnfSF",
             "TotalBsmtSF","1stFlrSF", "2ndFlrSF", "LowQualFinSF", 
             "GrLivArea","TotRmsAbvGrd","GarageCars","GarageArea",
             "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", 
             "ScreenPorch", "PoolArea","MiscVal")
  
target = "SalePrice"

train = train[, (num_vars) := lapply(.SD, as.numeric), .SDcols = num_vars]

skew_dt = as.data.frame(as.numeric(sapply(train[,.SD,.SDcols = num_vars], skewness)))
names(skew_dt) = c('skewness')
skew_dt$num_vars = num_vars
skew_dt  = as.data.table(skew_dt)
skew_dt = skew_dt[,.(num_vars,skewness)]
skew_dt$skewness[1] = skewness(train$LotFrontage,na.rm = T)
skew_dt$skewness[3] = skewness(train$MasVnrArea,na.rm = T)


mean_dt = as.data.frame(sapply(train[,.SD,.SDcols = num_vars], mean))
names(mean_dt) = c('mean')
mean_dt$num_vars = num_vars
mean_dt  = as.data.table(mean_dt)
mean_dt = mean_dt[,.(num_vars,mean)]
mean_dt$mean[1] = mean(train$LotFrontage,na.rm =T)
mean_dt$mean[3] = mean(train$MasVnrArea,na.rm =T)

median_dt = as.data.frame(sapply(train[,.SD,.SDcols = num_vars], median))
names(median_dt) = c('median')
median_dt$num_vars = num_vars
median_dt  = as.data.table(median_dt)
median_dt = median_dt[,.(num_vars,median)]
median_dt$median[1] = median(train$LotFrontage,na.rm =T)
median_dt$median[3] = median(train$MasVnrArea,na.rm =T)


sd_dt = as.data.frame(sapply(train[,.SD,.SDcols = num_vars], sd))
names(sd_dt) = c('sd')
sd_dt$num_vars = num_vars
sd_dt  = as.data.table(sd_dt)
sd_dt = sd_dt[,.(num_vars,sd)]
sd_dt$sd[1] = sd(train$LotFrontage,na.rm =T)
sd_dt$sd[3] = sd(train$MasVnrArea,na.rm =T)

na_count_dt = as.data.frame(sapply(train[,.SD,.SDcols = c(fact_vars,num_vars,date_field)], function(x) sum(is.na(x))))
names(na_count_dt) = c('na_count')
na_count_dt$vars = c(fact_vars,num_vars,date_field)
na_count_dt  = as.data.table(na_count_dt)
na_count_dt = na_count_dt[,.(vars,na_count)]
na_count_dt = na_count_dt[,na_perc := na_count/dim(train)[1]*100]
na_count_dt = na_count_dt[,to_drop := ifelse((na_count/dim(train)[1]*100>=30),1,0)]
class_desc = c(rep('factor',length(fact_vars)),rep('numeric',length(num_vars)),rep('date',length(date_field)))
na_count_dt = na_count_dt[,class_desc:= class_desc]

all_var_one_view = na_count_dt

setkey(all_var_one_view,vars)
setkey(mean_dt,num_vars)

all_var_one_view = merge(all_var_one_view,mean_dt,all.x = T,by.x = "vars",by.y = "num_vars")

setkey(median_dt,num_vars)

all_var_one_view = merge(all_var_one_view,median_dt,all.x = T,by.x = "vars",by.y = "num_vars")

setkey(sd_dt,num_vars)

all_var_one_view = merge(all_var_one_view,sd_dt,all.x = T,by.x = "vars",by.y = "num_vars")

setkey(skew_dt,num_vars)
all_var_one_view = merge(all_var_one_view,skew_dt,all.x = T,by.x = "vars",by.y = "num_vars")

to_drop = na_count_dt[to_drop==1,.(vars)]

col_to_drop = c()

for(i in 1:dim(to_drop)[1])
{
  col_to_drop[i] = as.character(to_drop[i])
}

for(i in 1:dim(all_var_one_view)[1])
{
  if(all_var_one_view$class_desc[i]=="factor" & (all_var_one_view$na_perc[i]<=30) && (all_var_one_view$na_perc[i]>0))
  {
    print("cleaning factor variables")
    print(all_var_one_view$vars[i])
    req_col = as.vector(train[,.SD,.SDcols = all_var_one_view$vars[i]])
    ind = which(is.na(req_col))
    req_col[ind] = "MISSING"
    train = train[,all_var_one_view$vars[i] := req_col]
  }
}
for(i in 1:dim(all_var_one_view)[1])
{
  if(all_var_one_view$class_desc[i]=="numeric" & (all_var_one_view$na_perc[i]<=30) && (all_var_one_view$na_perc[i]>0))
  {
    print("cleaning numeric variables")
    print(all_var_one_view$vars[i])
    req_col1 = as.vector(train[,.SD,.SDcols = all_var_one_view$vars[i]])
    ind = which(is.na(req_col1))
    median_val = all_var_one_view$median[i]
    req_col1[ind] = median_val
    train = train[,all_var_one_view$vars[i] := req_col1]
  }
}

all_var_one_view = all_var_one_view[,entropy_value := NA]

for(i in 1:dim(all_var_one_view)[1])
{
  if(all_var_one_view$class_desc[i] %in% c("factor","date"))
  {
    print("cleaning factor variables")
    print(all_var_one_view$vars[i])
    req_col = as.vector(train[,.SD,.SDcols = all_var_one_view$vars[i]])
    ent1 = as.vector(prop.table(table(req_col)))
    ent2 = (-1)*log(ent1)
    ent3 = ent1*ent2
    ent4 = sum(ent3)
    all_var_one_view$entropy_value[i] = ent4
  }
}

train = train[,LotFrontage_c := ifelse(LotFrontage <=59,'<=59', ifelse(LotFrontage<=69,'<=69',ifelse(LotFrontage<=80,'<=80','>80')))]
train = train[,LotArea_c := ifelse(LotArea <=7553.5,'<=7553.5', ifelse(LotArea<=9478.5,'<=9478.5',ifelse(LotArea<=11601.5,'<=11601.5','>11601.5')))]
train = train[,MasVnrArea_c := ifelse(MasVnrArea<=166,'<=166','>166')]
train = train[,BsmtFinSF1_c := ifelse(BsmtFinSF1<=383.5,'<=383.5',ifelse(BsmtFinSF1<=712.25,'<=712.25','>712.25'))]

train = train[,BsmtUnfSF_c := ifelse(BsmtUnfSF <=223,'<=223', ifelse(BsmtUnfSF<=477.5,'<=477.5',ifelse(BsmtUnfSF<=808,'<=808','>808')))]
train = train[,TotalBsmtSF_c := ifelse(TotalBsmtSF <=795.75,'<=795.75', ifelse(TotalBsmtSF<=991.5,'<=991.5',ifelse(TotalBsmtSF<=1298.25,'<=1298.25','>1298.25')))]
train = train[,FirstFlrSF_c := ifelse(`1stFlrSF` <=882,'<=882', ifelse(`1stFlrSF`<=1087,'<=1087',ifelse(`1stFlrSF`<=1391.25,'<=1391.25','>1391.25')))]

train = train[,GrLivArea_c := ifelse(GrLivArea <=1129.5,'<=1129.5', ifelse(GrLivArea<=1464,'<=1464',ifelse(GrLivArea<=1776.75,'<=1776.75','>1776.75')))]
train = train[,TotRmsAbvGrd_c := ifelse(TotRmsAbvGrd <=5,'<=5', ifelse(TotRmsAbvGrd<=6,'<=6',ifelse(TotRmsAbvGrd<=7,'<=7','>7')))]
train = train[,GarageCars_c := ifelse(GarageCars <=1,'<=1', ifelse(GarageCars<=2,'<=2',ifelse(GarageCars<=2,'<=2','>2')))]
train = train[,GarageArea_c := ifelse(GarageArea <=334.5,'<=334.5', ifelse(GarageArea<=480,'<=480',ifelse(GarageArea<=576,'<=576','>576')))]
train = train[,WoodDeckSF_c := ifelse(WoodDeckSF<=168,'<=168','>168')]
train = train[,OpenPorchSF_c := ifelse(OpenPorchSF<=25,'<=25',ifelse(OpenPorchSF<=68,'<=68','>68'))]

for(i in 1:dim(all_var_one_view)[1])
{
  if(all_var_one_view$class_desc[i]=="numeric" & all_var_one_view$vars[i] %nin% c('1stFlrSF','2ndFlrSF','3SsnPorch',
                                                                                  'BsmtFinSF2','EnclosedPorch','LowQualFinSF',
                                                                                  'MiscVal','PoolArea','ScreenPorch'))
  {
    print("cleaning numeric variables")
    print(all_var_one_view$vars[i])
    req_col_name = paste0(all_var_one_view$vars[i],"_c")
    req_col = as.vector(train[,.SD,.SDcols = req_col_name])
    ent1 = as.vector(prop.table(table(req_col)))
    ent2 = (-1)*log(ent1)
    ent3 = ent1*ent2
    ent4 = sum(ent3)
    all_var_one_view$entropy_value[i] = ent4
  }
}



train = train[,house_vintage := as.numeric(YrSold)- as.numeric(YearBuilt)]
train = train[,garage_vintage := as.numeric(YrSold)- as.numeric(GarageYrBlt)]
train = train[,time_since_remodelling := as.numeric(YrSold) - as.numeric(YearRemodAdd)]

train = train[, SalePrice := as.numeric(SalePrice)]
train = train[, SalePrice_log := log(SalePrice)]

mean_target = mean(train$SalePrice)
sd_target = sd(train$SalePrice)

train = train[,SalePrice_norm := (SalePrice-mean_target)/sd_target]

# train$TotalBsmtSF || train$`1stFlrSF`
# train$GrLivArea || train$1`1stFlrSF`
# train$`2ndFlrSF` || train$GrLivArea
# train$
 

for(i in 1:dim(all_var_one_view)[1])
{
  if(all_var_one_view$class_desc[i]=="numeric")
  {
    print("analysing the outliers")
    print(all_var_one_view$vars[i])
    req_col1 = as.vector(train[,.SD,.SDcols = all_var_one_view$vars[i]])
    ind = which(((req_col1- all_var_one_view$mean[i])/all_var_one_view$sd[i])>=3.5 | ((req_col1- all_var_one_view$mean[i])/all_var_one_view$sd[i])<= -3.5)
    median_val = all_var_one_view$median[i]
    req_col1[ind] = median_val
    train = train[,all_var_one_view$vars[i] := req_col1]
  }
}

cols_to_keep = names(train)[names(train) %nin% col_to_drop]

train = train[,.SD,.SDcols = cols_to_keep]


