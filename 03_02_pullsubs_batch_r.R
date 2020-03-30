### connection

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


#Defining the function 
#Subs Score from Justin Cress
sub_score_calc <- function(trans, min_xy=1) {
  hh_prod <- trans %>% select(hshd_id, brand) %>% distinct()
  
  #prod counts
  prod_counts <- hh_prod %>%
    group_by(brand) %>%
    summarise(hshd_n = n_distinct(hshd_id)) %>%
    ungroup()
  
  #count hshd_id by prod pair
  prod_pair <- hh_prod %>%
    left_join(hh_prod, by = c('hshd_id')) %>%
    group_by(brand.x, brand.y) %>%
    filter(brand.x != brand.y) %>%
    summarise(hshd_n = n_distinct(hshd_id)) %>%
    ungroup()
  
  rm(hh_prod)
  
  #count hshd_id by prod pair in the *same basket*
  prod_pair_same <- trans %>%
    inner_join(trans, by=c('hshd_id', 'transaction_fid')) %>%
    filter(brand.x != brand.y) %>%
    group_by(brand.x, brand.y) %>%
    summarise(hshd_n = n_distinct(hshd_id)) %>%
    ungroup()
  
  sub_calc <- prod_pair %>%
    rename(hshd_xy = hshd_n) %>%  #hshd_xy: hshd buying both x and y
    filter(hshd_xy >= min_xy) %>% 
    left_join(trans %>% #hshd_cat: hshd shopping cat
                transmute(brand.x = brand,
                          hshd_cat = n_distinct(hshd_id)) %>%
                distinct(),
              by = c('brand.x')) %>%
    left_join(prod_pair_same %>% #hshd_same: hshd buying x and y in one basket
                rename(hshd_same = hshd_n),
              by = c('brand.x', 'brand.y')) %>%
    left_join(prod_counts %>% #hshd_x: hshd buying x
                rename(brand.x = brand,
                       hshd_x = hshd_n),
              by = c('brand.x')) %>%
    left_join(prod_counts %>% #hshd_y: hshd buying y
                rename(brand.y = brand,
                       hshd_y = hshd_n),
              by = c('brand.y')) %>%
    mutate_if(is.integer, as.numeric) %>%
    #here's the relevant part
    mutate(hshd_same = replace(hshd_same, is.na(hshd_same), 0),
           #hshd_diff: hshd buying x and y only in different baskets
           hshd_diff = hshd_xy - hshd_same,
           #e_xy: expected hshd buying x and y
           e_xy = (hshd_x * hshd_y)/(hshd_cat),
           #e_diff: expected number of hshd buying in distinct baskets
           e_diff = e_xy * mean(hshd_diff/hshd_xy),
           #e_same: expected number of hshd buying in same basket
           e_same = e_xy * mean(hshd_same/hshd_xy),
           #partial: partial index
           partial = hshd_diff / e_diff) %>%
    filter(partial > 1) %>%
    mutate(
      chi_sq = case_when(
        #enforce min hshd buying both x and y
        hshd_xy >= min_xy ~ (hshd_diff - e_diff)^2/e_diff,
        #zero for prod with enough support otherwise
        hshd_xy < min_xy & hshd_x >= min_xy & hshd_y >= min_xy ~ 0,
        TRUE ~ NA_real_
      ),
      comp_chi_sq = case_when(
        #enforce min hshd buying both x and y
        hshd_xy >= min_xy ~ (hshd_same - e_same)^2/e_same,
        #zero for prod with enough support otherwise
        hshd_xy < min_xy & hshd_x >= min_xy & hshd_y >= min_xy ~ 0,
        TRUE ~ NA_real_
      )
    ) %>%
    filter(!is.na(chi_sq) | !is.na(comp_chi_sq),
           hshd_xy > min_xy)
  
  rm(list=c('prod_counts', 'prod_pair', 'prod_pair_same'))
  gc()
  
  sub_calc <- sub_calc %>%
    left_join(
      sub_calc %>%
        group_by(brand.x) %>%
        summarise(chi_sq_tot = sum(chi_sq, na.rm=TRUE),
                  comp_chi_sq_tot = sum(comp_chi_sq, na.rm=TRUE)),
      by = c('brand.x')
    ) %>%
    mutate(chi_sq_pct = chi_sq/chi_sq_tot,
           chi_dist = 1/chi_sq_pct,
           comp_chi_sq_pct = comp_chi_sq/comp_chi_sq_tot,
           comp_chi_dist = 1/comp_chi_sq_pct)
  
  return(sub_calc)
}



#Loop through year and category
result <- c()
for (i in 1:length(cat)) {
  for (j in 1:length(fis_quarter)) {
    trans0 <-  import_query(
      query =paste0(
        "select distinct BRAND as brand
                 , TRANSACTION_FID as transaction_fid
                 , HSHD_ID as hshd_id
          from ",subs_univ,"
          where category in ('", cat[i], "') and fis_quarter_id = '",fis_quarter[j],"'"),
      workspace = "an_rt_ws130", lower_names=TRUE)
    
    result0 <- sub_score_calc(trans = trans0, min_xy = 1)
    if(nrow(result0) == 0){
      print("data.frame is empty")
      print(cat[i])
      print(fis_quarter[j])
      result0[1,]<-0
      result0$category <- cat[i]
      result0$fis_quarter_id <- fis_quarter[j]
    }else{
      print("data.frame is not empty")
      print(cat[i])
      print(fis_quarter[j])
      result0<-result0
      result0$category <- cat[i]
      result0$fis_quarter_id <- fis_quarter[j]
    }
    result <- rbind(result0,result)
  }
}

export_table(RTable = result, ExaTable = subs_output, workspace = WORKSPACE_ws130)



