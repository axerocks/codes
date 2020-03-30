@00_00_parameters.sql

execute drop_table_if_it_exists(p_owner => user, p_table_name => '&sn._subs_input');
create table &sn._subs_input as 
select 
       distinct(prd.brand) as brand
      ,prd.category
      ,prd.fis_quarter_id
      ,prd.transaction_fid
      ,prd.hshd_id
     from &sn2._trans_kpis prd
     inner join
     &brand_cat_universe. cat 
     on prd.brand=cat.brand 
     and prd.category=cat.category 
     and prd.fis_quarter_id=cat.fis_quarter_id;


execute drop_table_if_it_exists(p_owner => user, p_table_name => '&sn2._trans_kpis');