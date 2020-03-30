

@00_00_parameters.sql


--Calculate metrics needed for later summaries--Purchasing target at least once
EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_summ');
CREATE TABLE &sn._vi_summ PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
    SELECT /*+ parallel(49) */ *
  FROM (  SELECT DISTINCT 
             hshd_id
          ,  brand
          ,  category
          ,  count(distinct case when item_qty>0 then transaction_fid else null end)  OVER (PARTITION BY hshd_id, category) AS hh_comm_visits
          ,  sum(net_spend_amt)               OVER (PARTITION BY hshd_id, category, brand) AS hh_upc_spend
          ,  sum(item_qty)                    OVER (PARTITION BY hshd_id, category, brand) AS hh_upc_units
          ,  count(distinct case when item_qty>0 then transaction_fid else null end)  OVER (PARTITION BY hshd_id, category, brand) AS hh_upc_visits
      FROM &sn._trans_kpis
       )
  WHERE hh_upc_spend  > 0
  and hh_upc_units > 0
    and hh_comm_visits >=2;

---------------------------------------------------------------------
-------------------------------------------------------
--1 Exclusivity
--------------------------------------------------------

EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_exclu_0');  
CREATE TABLE &sn._vi_exclu_0 PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
  select /*+ parallel(49)*/ distinct 
   hshd_id
  ,category
  ,brand
  ,hh_upc_spend
  ,hh_upc_units
  ,count(brand) over (partition by hshd_id,category) as hh_num_upcs
from &sn._vi_summ;

  

EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_exclu'); 
CREATE TABLE &sn._vi_exclu PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
select brand
        ,category
        ,COUNT(CASE WHEN hh_num_upcs= 1 then 1 ELSE NULL END) as "exclusive_hhs"
        ,count(distinct hshd_id) as "total_hhs"
        ,COUNT(CASE WHEN hh_num_upcs= 1 then 1 ELSE NULL END)/count(*) as exclusive_Share
from &sn._vi_exclu_0
group by 
brand
,category;

EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_exclu_0');  


----------------------------------------------------------------------
-------------------------------------------------------
--2. Favorite Share
--------------------------------------------------------
EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_fav_eng_pct');
CREATE TABLE &sn._vi_fav_eng_pct PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
select /*+ parallel(49)*/ distinct 
   hshd_id
  ,category
  ,brand
  ,hh_upc_spend
  ,hh_upc_spend  / nullif((sum(hh_upc_spend)  over (partition by  hshd_id,  category)),0) AS pct_hh_sub_spend
  ,hh_upc_units  / nullif((sum(hh_upc_units)  over (partition by hshd_id,  category)),0) AS pct_hh_sub_units
  ,hh_upc_visits / nullif((sum(hh_upc_visits) over (partition by hshd_id,  category)),0) AS pct_hh_sub_visits
from &sn._vi_summ;



--add the pcts across engagement metrics for a rank_summ
  EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_fav_rank_summ');
CREATE TABLE &sn._vi_fav_rank_summ PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
select /*+ parallel(49)*/ distinct 
 hshd_id
 ,category
 ,brand
 ,hh_upc_spend
 ,sum(pct_hh_sub_spend + pct_hh_sub_units + pct_hh_sub_visits) AS rank_summ
from &sn._vi_fav_eng_pct
group by 
   hshd_id
  ,category
  ,brand
  ,hh_upc_spend
  ;

EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_fav_eng_pct');
EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_fav_rank_summ');
/*************** Drop intermediate table: ***************/
--EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_fav_eng_pct');
/********************************************************/
--rank products by subcommodity
EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_product_rank');
CREATE TABLE &sn._vi_product_rank PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
select /*+ parallel(49)*/ distinct 
   hshd_id
  ,category
  ,brand
  ,hh_upc_spend
  ,DENSE_RANK() OVER (PARTITION BY hshd_id, category ORDER BY rank_summ DESC) AS product_rank
from &sn._vi_fav_rank_summ;


/*************** Drop intermediate table: ***************/
--EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_fav_rank_summ');
/********************************************************/


--find pct favorite spend by brand
EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_upc_fav_spend');
CREATE TABLE &sn._vi_upc_fav_spend PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
select /*+ parallel(49)*/ distinct 
   category
  ,brand
  ,SUM(CASE WHEN product_rank=1 THEN hh_upc_spend ELSE 0 END) OVER(PARTITION BY category,brand) AS upc_fav_spend
  ,SUM(hh_upc_spend) OVER(PARTITION BY category,brand)  AS upc_tot_spend
  ,SUM(CASE WHEN product_rank=1 THEN hh_upc_spend ELSE 0 END) OVER(PARTITION BY category,brand)/SUM(hh_upc_spend) OVER(PARTITION BY category,brand) as fav_Share_ratio
FROM &sn._vi_product_rank;


EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_product_rank');

-----------------------
-- 3: SHARE OF SALES --
-----------------------
--Eliminate the HH element to get distinct overall UPC sales b   y store group
EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_sos_upc_sales_0');  
CREATE TABLE &sn._vi_sos_upc_sales_0 PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
  select /*+ parallel(49)*/ distinct 
   hshd_id
  ,category
  ,brand
  ,hh_upc_spend
  ,sum(hh_upc_spend) over (partition by hshd_id,category) as hh_cat_spend
  ,hh_upc_units 
  ,sum(hh_upc_units) over (partition by hshd_id,category) as hh_cat_units
from &sn._vi_summ;
  

/*************** Drop intermediate tables ***************/
#EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_sales_share_00');
#EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_sos_comm_rank');
/********************************************************/
--Finalize sales share data
EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_sales_share');
CREATE TABLE &sn._vi_sales_share PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
select /*+ parallel (49)*/ 
   category
  ,brand
  ,sum(hh_upc_spend)/sum(hh_cat_spend) as sor_revenue
  ,sum(hh_upc_units)/sum(hh_cat_units) as sor_units
from  &sn._vi_sos_upc_sales_0 
group by category
		   ,brand;

EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_sos_upc_sales_0'); 


 EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._fav_sales_share0');
CREATE TABLE &sn._fav_sales_share0 PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
select /*+ parallel (49)*/ 
   a.category
  ,a.brand
  ,sor_revenue
  ,sor_units
  ,upc_fav_spend 
  ,upc_tot_spend
  ,fav_Share_ratio
  ,exclusive_share
from  &sn._vi_sales_share  a
inner join 
&sn._vi_upc_fav_spend  b
on a.brand=b.brand 
and a.category=b.category
inner join 
&sn._vi_exclu  c
on a.brand=c.brand 
and a.category=c.category;


EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_sales_share');
EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_upc_fav_spend');
EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._vi_exclu');



 EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._fav_sales_share');
CREATE TABLE &sn._fav_sales_share PARALLEL COMPRESS FOR QUERY LOW TABLESPACE WH_AGG_SCRATCH_OL AS
select /*+ parallel (49)*/ 
   a.category
  ,a.brand
  ,a.fis_quarter_id
  ,sor_revenue
  ,sor_units
  ,upc_fav_spend 
  ,upc_tot_spend
  ,fav_Share_ratio
  ,exclusive_share
  ,avg_sor_revenue
  ,avg_sor_units
  ,avg_fav_share_ratio
  ,avg_exclusive_share
  ,sor_revenue/avg_sor_revenue as ind_sor_revenue
  ,sor_units/avg_sor_units as ind_sor_units
  ,fav_share_ratio/avg_fav_share_ratio as ind_fav_share_ratio
  ,exclusive_share/avg_exclusive_share as ind_exclusive_share
from  &sn._fav_sales_share0 a 
inner join 
  (select category
          ,fis_quarter_id
          ,avg(sor_revenue) as avg_sor_revenue
          ,avg(sor_units) as avg_sor_units
          ,avg(fav_share_ratio) as avg_fav_share_ratio
          ,avg(exclusive_share) as avg_exclusive_share
    from &sn._fav_sales_share0
    group by category
              ,fis_Quarter_id) b
  on a.category=b.category
  and a.fis_quarter_id=b.fis_Quarter_id;

 EXECUTE drop_table_if_it_exists( p_owner => USER , p_table_name => '&sn._fav_sales_share0');