@00_00_parameters.sql



--HH/Visit/Wk/Cat/Brand ---------------------------------------------------------------------------------------------------------

execute drop_table_if_it_exists(p_owner => user, p_table_name => '&sn2._trans_kpis');
create table &sn2._trans_kpis compress for query high parallel as
select hshd_id
     ,transaction_fid
     ,store_id
     ,fis_week_id
     ,fis_quarter_id
     ,category
     ,brand
     ,converted_hshd_id
     ,sum(net_spend_amt) as spend
     ,sum(item_qty) as units
from &sn._trans_kpis
group by hshd_id
       ,transaction_fid
       ,store_id
       ,fis_week_id
       ,fis_quarter_id
       ,category
       ,brand
       ,converted_hshd_id
having sum(item_qty)>0
        ;
EXECUTE dbms_stats.gather_table_stats (ownname =>USER, tabname => '&sn2._trans_kpis' , degree => 4);





--------Brand Totals  ---------------------------------------------------------------------------------------------------------
execute drop_table_if_it_exists(p_owner => user, p_table_name => '&sn2._brand_kpis');
create table &sn2._brand_kpis compress for query high parallel as
select category
     ,brand
     ,fis_quarter_id
     ,sum(spend) as brand_spend
     ,sum(units) as brand_units
     ,count(distinct hshd_id) as brand_hhs 
from &sn2._trans_kpis 
group by category
       ,brand
       ,fis_quarter_id
;
EXECUTE dbms_stats.gather_table_stats (ownname =>USER, tabname => '&sn2._brand_kpis' , degree => 4);
------------------------------------------------------------------------------------------------------------------------------

----------Brand Visits and Repeats -------------------------------------------------------------------------------------------
execute drop_table_if_it_exists(p_owner => user, p_table_name => '&sn2._visit_repeat');
create table &sn2._visit_repeat compress for query high parallel as
select category
     ,brand
     ,fis_quarter_id
     ,sum(visits) as visits
     ,count(distinct case when visits>1 then hshd_id else null end) as brand_repeats
     ,count(distinct case when visits>2 then hshd_id else null end) as brand_repeaters
from (
       select category
             ,brand
             ,hshd_id
             ,fis_quarter_id
             ,count(distinct transaction_fid) as visits
       from &sn2._trans_kpis
       group by category, brand, fis_quarter_id, hshd_id
    )
group by category,brand,fis_quarter_id;
EXECUTE dbms_stats.gather_table_stats (ownname =>USER, tabname => '&sn2._visit_repeat' , degree => 4);

------------------------------------------------------------------------------------------------------------------------------
----------Repeat Rate Computation -------------------------------------------------------------------------------------------
execute drop_table_if_it_exists(p_owner => user, p_table_name => '&rr_table');
create table &rr_table compress for query high parallel as
select a.brand
      ,a.category
      ,a.fis_quarter_id
      ,a.brand_hhs
      ,c.brand_repeats
      ,c.brand_repeaters
      ,case when brand_hhs=0 then 0 else brand_repeats/brand_hhs end as brand_repeatrate_adj
from &sn2._brand_kpis a
left join &sn2._visit_repeat c
    on a.category=c.category
    and a.brand=c.brand
    and a.fis_quarter_id=c.fis_quarter_id
order by a.category,a.brand,a.fis_quarter_id;
-- ;
EXECUTE dbms_stats.gather_table_stats (ownname =>USER, tabname => '&rr_table' , degree => 4);


EXECUTE drop_table_if_it_exists( p_owner => USER, p_table_name => '&sn2._item_store_week');
create table &sn2._item_store_week compress for query high parallel as
select brand, 
      category,
      fis_quarter_id,
      fis_week_id,
      store_id, 
      sum(units) as units
from &sn2._trans_kpis
group by brand, 
        category, 
        fis_quarter_id,
        fis_week_id,
        store_id
having sum(units)>0;
EXECUTE dbms_stats.gather_table_stats(ownname=>USER, tabname =>'&sn2._item_store_week',degree=>DBMS_STATS.AUTO_DEGREE);


EXECUTE drop_table_if_it_exists( p_owner => USER, p_table_name => '&sn2._hh_store_week');
create table &sn2._hh_store_week compress for query high parallel as
select distinct fis_week_id,
                fis_quarter_id, 
                store_id, 
                hshd_id
from &sn2._trans_kpis
where units>0;
EXECUTE dbms_stats.gather_table_stats(ownname=>USER, tabname =>'&sn2._hh_store_week',degree=>DBMS_STATS.AUTO_DEGREE);


