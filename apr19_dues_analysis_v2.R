apr_19_non_13thmonth_banca = fread("/hadoop/scoring/non_monthly/evaluation/Banca_non13th_nm_apr19_renewals_eval.csv",sep=",",colClasses = "character")
apr_19_non_13thmonth_nbanca = fread("/hadoop/scoring/non_monthly/evaluation/Non_Banca_non13th_nm_apr19_renewals_eval.csv",sep=",",colClasses = "character")
apr_19_13th_banca = fread("/hadoop/scoring/non_monthly/evaluation/Banca_13th_nm_apr19_renewals_eval.csv",sep=",",colClasses = "character")
apr_19_13th_nbanca = fread("/hadoop/scoring/non_monthly/evaluation/Non_Banca_13th_nm_apr19_renewals_eval.csv",sep=",",colClasses = "character")
apr_19_non_13th_monthly = fread("/hadoop/scoring/non_monthly/evaluation/all_m_apr19_renewal_eval.csv",sep=",",colClasses = "character")


all_dt = rbindlist(list(apr_19_non_13thmonth_banca,apr_19_non_13thmonth_nbanca,apr_19_13th_banca,apr_19_13th_nbanca,apr_19_non_13th_monthly))


write.table(all_dt,"/hadoop/datascience/internal_work/apr19_dues_analysis_input_2",sep = "|",quote = FALSE,row.names = FALSE)

#hive -e 'set hive.cli.print.header=true; select * from aureus_temp.out_apr19 ' | sed 's/[\t]/|/g'  > out_apr19 

all_dt_out = fread("/hadoop/datascience/internal_work/out_apr19",sep = "|",colClasses = "character")
names(all_dt_out) = gsub("out_apr19.","",names(all_dt_out))

all_dt_out = all_dt_out[,rag_grouping_2 := ifelse(rag_grouping %in% c('AMBER1','AMBER2'),'AMBER',
                                                  ifelse(rag_grouping %in% c('DARK RED','LIGHT RED'),'RED',rag_grouping))]

all_dt_out = all_dt_out[,Zone := ifelse(serviceregionnamemktg %in% c('Ahmedabad','Chandigarh','Delhi','Jaipur','Mumbai'),'Zone_1',
                                                          ifelse(serviceregionnamemktg %in% c('Andhra Pradesh','Bangalore','Chennai','Kerala','Telangana'),'Zone_2',
                                                                 ifelse(serviceregionnamemktg %in% c('Bengal','Bhopal','Bhubaneswar','Lucknow','North East','Patna'),'Zone_3','Others')))]

all_dt_out = all_dt_out[,Overall_zone := ifelse(serviceregionnamemktg %in% c('Ahmedabad','Chandigarh','Delhi','Jaipur','Mumbai',
                                                                                               'Andhra Pradesh','Bangalore','Chennai','Kerala','Telangana',
                                                                                               'Bengal','Bhopal','Bhubaneswar','Lucknow','North East','Patna'),'Total','Others')]

View(all_dt_out[,.(GREEN=length(proposal_no[rag_grouping_2=='GREEN']),
              AMBER=length(proposal_no[rag_grouping_2=='AMBER']),
              RED=length(proposal_no[rag_grouping_2=='RED'])),by = model_name])

View(all_dt_out[,.(GREEN=length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES']),
                   AMBER=length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES']),
                   RED=length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])),by = model_name])

View(all_dt_out[,.(GREEN=length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                   AMBER=length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                   RED=length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED'])),by = model_name])

final_dt = data.table()

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Ahmedabad',.(Zone = 'Zone_1',Region = 'Ahmedabad',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                          RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                          AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                          AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                          GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                          GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                          GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                          AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                          RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                          Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Chandigarh',.(Zone = 'Zone_1',Region = 'Chandigarh',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                           RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                           AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                           AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                           GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                           GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                           GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                           AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                           RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                           Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Delhi',.(Zone = 'Zone_1',Region = 'Delhi',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                      RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                      AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                      AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                      GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                      GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                      GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                      AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                      RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                      Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Jaipur',.(Zone = 'Zone_1',Region = 'Jaipur',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                       RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                       AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                       GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                       AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                       RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                       Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Mumbai',.(Zone = 'Zone_1',Region = 'Mumbai',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                       RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                       AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                       GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                       AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                       RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                       Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg %in% c('Ahmedabad','Chandigarh','Delhi','Jaipur','Mumbai')
                                     ,.(Zone = 'Zone_1_total',Region = 'Zone_1',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                        RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                        AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                        GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                        AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                        RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                        Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Andhra Pradesh',.(Zone = 'Zone_2',Region = 'Andhra Pradesh',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                               RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                               AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                               AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                               GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                               GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                               GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                               AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                               RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                               Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])


final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Bangalore',.(Zone = 'Zone_2',Region = 'Bangalore',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                          RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                          AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                          AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                          GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                          GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                          GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                          AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                          RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                          Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Chennai',.(Zone = 'Zone_2',Region = 'Chennai',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                        RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                        AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                        AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                        GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                        GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                        GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                        AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                        RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                        Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Kerala',.(Zone = 'Zone_2',Region = 'Kerala',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                       RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                       AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                       GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                       AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                       RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                       Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Telangana',.(Zone = 'Zone_2',Region = 'Telangana',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                          RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                          AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                          AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                          GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                          GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                          GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                          AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                          RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                          Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg %in% c('Andhra Pradesh','Bangalore','Chennai','Kerala','Telangana')
                                     ,.(Zone = 'Zone_2_total',Region = 'Zone_2',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                        RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                        AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                        GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                        AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                        RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                        Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Bengal',.(Zone = 'Zone_3',Region = 'Bengal',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                       RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                       AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                       GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                       AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                       RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                       Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Bhopal',.(Zone = 'Zone_3',Region = 'Bhopal',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                       RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                       AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                       GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                       GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                       AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                       RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                       Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Bhubaneswar',.(Zone = 'Zone_3',Region = 'Bhubaneswar',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                            RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                            AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                            AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                            GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                            GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                            GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                            AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                            RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                            Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Lucknow',.(Zone = 'Zone_3',Region = 'Lucknow',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                        RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                        AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                        AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                        GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                        GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                        GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                        AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                        RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                        Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='North East',.(Zone = 'Zone_3',Region = 'North East',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                           RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                           AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                           AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                           GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                           GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                           GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                           AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                           RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                           Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg=='Patna',.(Zone = 'Zone_3',Region = 'Patna',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                                                      RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                      AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                                                      AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                      GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                                                      GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                                                      GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                                                      AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                                                      RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                                                      Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg %in% c('Bengal','Bhopal','Bhubaneswar','Lucknow','North East','Patna')
                                     ,.(Zone = 'Zone_3_total',Region = 'Zone_3',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                        RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                        AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                        GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                        AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                        RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                        Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])

final_dt = rbind(final_dt,all_dt_out[serviceregionnamemktg %in% c('Ahmedabad','Chandigarh','Delhi','Jaipur','Mumbai',
                                                                  'Andhra Pradesh','Bangalore','Chennai','Kerala','Telangana',
                                                                  'Bengal','Bhopal','Bhubaneswar','Lucknow','North East','Patna')
                                     ,.(Zone = 'Total',Region = 'All_Zone',RED_Estimated = length(proposal_no[rag_grouping_2=='RED'])/length(proposal_no),
                                        RED_actual = length(proposal_no[rag_grouping_2=='RED' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        AMBER_Estimated = length(proposal_no[rag_grouping_2=='AMBER'])/length(proposal_no),
                                        AMBER_actual = length(proposal_no[rag_grouping_2=='AMBER' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        GREEN_Estimated = length(proposal_no[rag_grouping_2=='GREEN'])/length(proposal_no),
                                        GREEN_actual = length(proposal_no[rag_grouping_2=='GREEN' & paid == 'YES'])/length(proposal_no[paid=='YES']),
                                        GREEN_renewal = length(proposal_no[rag_grouping_2=='GREEN' & paid=='YES'])/length(proposal_no[rag_grouping_2=='GREEN']),
                                        AMBER_renewal = length(proposal_no[rag_grouping_2=='AMBER' & paid=='YES'])/length(proposal_no[rag_grouping_2=='AMBER']),
                                        RED_renewal = length(proposal_no[rag_grouping_2=='RED' & paid=='YES'])/length(proposal_no[rag_grouping_2=='RED']),
                                        Overall_renewal = length(proposal_no[paid=='YES'])/length(proposal_no))])


View(final_dt)
