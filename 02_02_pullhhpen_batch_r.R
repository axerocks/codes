
source("00_00_parameters.R")

setwd(prog_dir)



category_list <- import_query(
  query =paste0(
    " select distinct(category)
  ,fis_quarter_id
  from ",cat_univ,""),
  workspace = WORKSPACE_ws130)



cat<- unique(category_list$CATEGORY)
fis_quarter<- unique(category_list$FIS_QUARTER_ID)


result <- c()
for (i in 1:length(cat)) {
  for (j in 1:length(fis_quarter)) {
    define_sql_parameters(file=paste0(prog_dir,"/02_parameters.sql"),
                          proxy_user=workspace,
                          fis_quarter_id=paste0("'",fis_quarter[j],"'"),
                          category= paste0("'",cat[i],"'"),
                          final_table=paste0("sn_metrics_",i,j))
    sqlbatch(filename=paste0(prog_dir,"/02_03_pull_hhpen_brand.sql"),proxy="AN_RT_WS130",inst="exa_uskrgprdh")
     result0 <- import_table(
      table = paste0("sn_metrics_",i,j),
      workspace = "an_rt_ws130")
    if(nrow(result0) == 0){
      warning("data.frame is empty")
      warning(cat[i])
      warning(fis_quarter[j])
    }else{
      print("data.frame is not empty")
      print(cat[i])
      print(fis_quarter[j])
      result0<-result0
      result <- rbind(result0,result)
    }
   
  }
}
export_table(RTable = result, ExaTable = hh_den, workspace = WORKSPACE_ws130)



