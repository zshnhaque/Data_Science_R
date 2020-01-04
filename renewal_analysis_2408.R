# hive -e 'set hive.cli.print.header=true; select * from aureus_temp.pol_dt14' | sed 's/[\t]/|/g'  > pol_dt14;

dt = fread('/hadoop/datascience/internal_work/sep2019/pol_dt14',sep = '|',colClasses = 'character',na.strings = c('NULL','None','',' ',NA))
names(dt) = gsub("pol_dt14.","",names(dt))

dt = dt[,policyannualizedpremium:=as.numeric(policyannualizedpremium)]
dt = dt[,status_as_of_mar18:=ifelse(is.na(status_as_of_mar18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2018-04-01'),
                                    'Terminated or cannot be revived',status_as_of_mar18)]
dt = dt[,status_as_of_apr18:=ifelse(is.na(status_as_of_apr18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2018-05-01'),
                                    'Terminated or cannot be revived',status_as_of_apr18)]
dt = dt[,status_as_of_may18:=ifelse(is.na(status_as_of_may18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2018-06-01'),
                                    'Terminated or cannot be revived',status_as_of_may18)]
dt = dt[,status_as_of_june18:=ifelse(is.na(status_as_of_june18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2018-07-01'),
                                    'Terminated or cannot be revived',status_as_of_june18)]
dt = dt[,status_as_of_july18:=ifelse(is.na(status_as_of_july18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2018-08-01'),
                                    'Terminated or cannot be revived',status_as_of_july18)]
dt = dt[,status_as_of_aug18:=ifelse(is.na(status_as_of_aug18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2018-09-01'),
                                    'Terminated or cannot be revived',status_as_of_aug18)]
dt = dt[,status_as_of_sept18:=ifelse(is.na(status_as_of_sept18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2018-10-01'),
                                    'Terminated or cannot be revived',status_as_of_sept18)]
dt = dt[,status_as_of_oct18:=ifelse(is.na(status_as_of_oct18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2018-11-01'),
                                    'Terminated or cannot be revived',status_as_of_oct18)]
dt = dt[,status_as_of_nov18:=ifelse(is.na(status_as_of_nov18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2018-12-01'),
                                    'Terminated or cannot be revived',status_as_of_nov18)]
dt = dt[,status_as_of_dec18:=ifelse(is.na(status_as_of_dec18)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2019-01-01'),
                                    'Terminated or cannot be revived',status_as_of_dec18)]
dt = dt[,status_as_of_jan19:=ifelse(is.na(status_as_of_jan19)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2019-02-01'),
                                    'Terminated or cannot be revived',status_as_of_jan19)]
dt = dt[,status_as_of_feb19:=ifelse(is.na(status_as_of_feb19)==TRUE & status_as_of_sept2019 %in% c('Terminated','Maturity','Withdrawal') & as.Date(policycurrentstatusseffectdate_sept2019)<as.Date('2019-03-01'),
                                    'Terminated or cannot be revived',status_as_of_feb19)]

dt_out = data.table()
dt_out2 = data.table()

dt_out = rbind(dt_out,dt[,.(month='Mar-18',inforce=length(policytechnicalid[status_as_of_mar18=='Inforce' & is.na(status_as_of_mar18)==FALSE]),
           inforce_ap=sum(policyannualizedpremium[status_as_of_mar18=='Inforce' & is.na(status_as_of_mar18)==FALSE],na.rm = T),
      tech_lapsed=length(policytechnicalid[status_as_of_mar18=='Tech Lapsed' & is.na(status_as_of_mar18)==FALSE]),
      tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_mar18=='Tech Lapsed' & is.na(status_as_of_mar18)==FALSE],na.rm = T),
      lapsed=length(policytechnicalid[status_as_of_mar18=='Lapsed' & is.na(status_as_of_mar18)==FALSE]),
      lapsed_ap=sum(policyannualizedpremium[status_as_of_mar18=='Lapsed' & is.na(status_as_of_mar18)==FALSE],na.rm = T),
      terminated=length(policytechnicalid[status_as_of_mar18=='Terminated or cannot be revived' & is.na(status_as_of_mar18)==FALSE]),
      terminated_ap=sum(policyannualizedpremium[status_as_of_mar18=='Terminated or cannot be revived' & is.na(status_as_of_mar18)==FALSE],na.rm = T),
      surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-03' & is.na(surrender_dt_sel)==FALSE]),
      surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-03' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
      claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-03' & status_as_of_sept2019=='Claim']),
      claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-03' & status_as_of_sept2019=='Claim'],na.rm = T),
      matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-03' & status_as_of_sept2019=='Maturity']),
      matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-03' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='Apr-18',inforce=length(policytechnicalid[status_as_of_apr18=='Inforce' & is.na(status_as_of_apr18)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_apr18=='Inforce' & is.na(status_as_of_apr18)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_apr18=='Tech Lapsed' & is.na(status_as_of_apr18)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_apr18=='Tech Lapsed' & is.na(status_as_of_apr18)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_apr18=='Lapsed']),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_apr18=='Lapsed'& is.na(status_as_of_apr18)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_apr18=='Terminated or cannot be revived' & is.na(status_as_of_apr18)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_apr18=='Terminated or cannot be revived' & is.na(status_as_of_apr18)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-04' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-04' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-04' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-04' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-04' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-04' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='May-18',inforce=length(policytechnicalid[status_as_of_may18=='Inforce' & is.na(status_as_of_may18)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_may18=='Inforce' & is.na(status_as_of_may18)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_may18=='Tech Lapsed' & is.na(status_as_of_may18)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_may18=='Tech Lapsed' & is.na(status_as_of_may18)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_may18=='Lapsed' & is.na(status_as_of_may18)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_may18=='Lapsed' & is.na(status_as_of_may18)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_may18=='Terminated or cannot be revived' & is.na(status_as_of_may18)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_may18=='Terminated or cannot be revived' & is.na(status_as_of_may18)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-05' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-05' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-05' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-05' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-05' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-05' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='June-18',inforce=length(policytechnicalid[status_as_of_june18=='Inforce' & is.na(status_as_of_june18)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_june18=='Inforce' & is.na(status_as_of_june18)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_june18=='Tech Lapsed' & is.na(status_as_of_june18)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_june18=='Tech Lapsed' & is.na(status_as_of_june18)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_june18=='Lapsed' & is.na(status_as_of_june18)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_june18=='Lapsed' & is.na(status_as_of_june18)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_june18=='Terminated or cannot be revived' & is.na(status_as_of_june18)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_june18=='Terminated or cannot be revived' & is.na(status_as_of_june18)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-06' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-06' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-06' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-06' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-06' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-06' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='July-18',inforce=length(policytechnicalid[status_as_of_july18=='Inforce' & is.na(status_as_of_july18)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_july18=='Inforce' & is.na(status_as_of_july18)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_july18=='Tech Lapsed' & is.na(status_as_of_july18)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_july18=='Tech Lapsed' & is.na(status_as_of_july18)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_july18=='Lapsed' & is.na(status_as_of_july18)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_july18=='Lapsed' & is.na(status_as_of_july18)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_july18=='Terminated or cannot be revived' & is.na(status_as_of_july18)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_july18=='Terminated or cannot be revived' & is.na(status_as_of_july18)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-07' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-07' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-07' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-07' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-07' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-07' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='Aug-18',inforce=length(policytechnicalid[status_as_of_aug18=='Inforce' & is.na(status_as_of_aug18)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_aug18=='Inforce' & is.na(status_as_of_aug18)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_aug18=='Tech Lapsed' & is.na(status_as_of_aug18)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_aug18=='Tech Lapsed' & is.na(status_as_of_aug18)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_aug18=='Lapsed' & is.na(status_as_of_aug18)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_aug18=='Lapsed' & is.na(status_as_of_aug18)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_aug18=='Terminated or cannot be revived' & is.na(status_as_of_aug18)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_aug18=='Terminated or cannot be revived' & is.na(status_as_of_aug18)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-08' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-08' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-08' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-08' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-08' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-08' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='Sept-18',inforce=length(policytechnicalid[status_as_of_sept18=='Inforce' & is.na(status_as_of_sept18)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_sept18=='Inforce' & is.na(status_as_of_sept18)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_sept18=='Tech Lapsed' & is.na(status_as_of_sept18)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_sept18=='Tech Lapsed' & is.na(status_as_of_sept18)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_sept18=='Lapsed' & is.na(status_as_of_sept18)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_sept18=='Lapsed' & is.na(status_as_of_sept18)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_sept18=='Terminated or cannot be revived' & is.na(status_as_of_sept18)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_sept18=='Terminated or cannot be revived' & is.na(status_as_of_sept18)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-09' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-09' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-09' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-09' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-09' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-09' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='Oct-18',inforce=length(policytechnicalid[status_as_of_oct18=='Inforce' & is.na(status_as_of_oct18)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_oct18=='Inforce' & is.na(status_as_of_oct18)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_oct18=='Tech Lapsed' & is.na(status_as_of_oct18)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_oct18=='Tech Lapsed' & is.na(status_as_of_oct18)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_oct18=='Lapsed' & is.na(status_as_of_oct18)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_oct18=='Lapsed' & is.na(status_as_of_oct18)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_oct18=='Terminated or cannot be revived' & is.na(status_as_of_oct18)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_oct18=='Terminated or cannot be revived' & is.na(status_as_of_oct18)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-10' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-10' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-10' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-10' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-10' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-10' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='Nov-18',inforce=length(policytechnicalid[status_as_of_nov18=='Inforce' & is.na(status_as_of_nov18)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_nov18=='Inforce' & is.na(status_as_of_nov18)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_nov18=='Tech Lapsed' & is.na(status_as_of_nov18)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_nov18=='Tech Lapsed' & is.na(status_as_of_nov18)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_nov18=='Lapsed' & is.na(status_as_of_nov18)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_nov18=='Lapsed' & is.na(status_as_of_nov18)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_nov18=='Terminated or cannot be revived' & is.na(status_as_of_nov18)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_nov18=='Terminated or cannot be revived' & is.na(status_as_of_nov18)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-11' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-11' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-11' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-11' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-11' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-11' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='Dec-18',inforce=length(policytechnicalid[status_as_of_dec18=='Inforce' & is.na(status_as_of_dec18)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_dec18=='Inforce'& is.na(status_as_of_dec18)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_dec18=='Tech Lapsed' & is.na(status_as_of_dec18)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_dec18=='Tech Lapsed' & is.na(status_as_of_dec18)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_dec18=='Lapsed' & is.na(status_as_of_dec18)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_dec18=='Lapsed' & is.na(status_as_of_dec18)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_dec18=='Terminated or cannot be revived' & is.na(status_as_of_dec18)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_dec18=='Terminated or cannot be revived' & is.na(status_as_of_dec18)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2018-12' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2018-12' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-12' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-12' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-12' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2018-12' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='Jan-19',inforce=length(policytechnicalid[status_as_of_jan19=='Inforce' & is.na(status_as_of_jan19)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_jan19=='Inforce'& is.na(status_as_of_jan19)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_jan19=='Tech Lapsed'& is.na(status_as_of_jan19)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_jan19=='Tech Lapsed'& is.na(status_as_of_jan19)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_jan19=='Lapsed'& is.na(status_as_of_jan19)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_jan19=='Lapsed'& is.na(status_as_of_jan19)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_jan19=='Terminated or cannot be revived'& is.na(status_as_of_jan19)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_jan19=='Terminated or cannot be revived'& is.na(status_as_of_jan19)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2019-01' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2019-01' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2019-01' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2019-01' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2019-01' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2019-01' & status_as_of_sept2019=='Maturity'],na.rm = T))])

dt_out = rbind(dt_out,dt[,.(month='Feb-19',inforce=length(policytechnicalid[status_as_of_feb19=='Inforce' & is.na(status_as_of_feb19)==FALSE]),
                            inforce_ap=sum(policyannualizedpremium[status_as_of_feb19=='Inforce' & is.na(status_as_of_feb19)==FALSE],na.rm = T),
                            tech_lapsed=length(policytechnicalid[status_as_of_feb19=='Tech Lapsed' & is.na(status_as_of_feb19)==FALSE]),
                            tech_lapsed_ap=sum(policyannualizedpremium[status_as_of_feb19=='Tech Lapsed' & is.na(status_as_of_feb19)==FALSE],na.rm = T),
                            lapsed=length(policytechnicalid[status_as_of_feb19=='Lapsed' & is.na(status_as_of_feb19)==FALSE]),
                            lapsed_ap=sum(policyannualizedpremium[status_as_of_feb19=='Lapsed' & is.na(status_as_of_feb19)==FALSE],na.rm = T),
                            terminated=length(policytechnicalid[status_as_of_feb19=='Terminated or cannot be revived' & is.na(status_as_of_feb19)==FALSE]),
                            terminated_ap=sum(policyannualizedpremium[status_as_of_feb19=='Terminated or cannot be revived' & is.na(status_as_of_feb19)==FALSE],na.rm = T),
                            surrendered = length(policytechnicalid[substr(surrender_dt_sel,1,7)=='2019-02' & is.na(surrender_dt_sel)==FALSE]),
                            surrendered_ap = sum(policyannualizedpremium[substr(surrender_dt_sel,1,7)=='2019-02' & is.na(surrender_dt_sel)==FALSE],na.rm = T),
                            claim = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2019-02' & status_as_of_sept2019=='Claim']),
                            claim_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2019-02' & status_as_of_sept2019=='Claim'],na.rm = T),
                            matured = length(policytechnicalid[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2019-02' & status_as_of_sept2019=='Maturity']),
                            matured_ap = sum(policyannualizedpremium[substr(policycurrentstatusseffectdate_sept2019,1,7)=='2019-02' & status_as_of_sept2019=='Maturity'],na.rm = T))])
