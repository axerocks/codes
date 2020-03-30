@00_00_parameters.sql
@02_parameters.sql





------------------------------------------LOOP----------------------------------------------------------------------------------
--Computing HH Penetration --where category in (&category) and fis_quarter_id in (&fis_quarter_id)
------Brand Store Weeks --------------------------------------------------------------------------------------------------------

--Need to create an item||week table for adjusted HH pen;

--Calculate HH Pen denominator for each item (distinct households shopping in the weeks that item was sold);
EXECUTE drop_table_if_it_exists( p_owner => USER, p_table_name => '&final_table');
create table &final_table compress for query high parallel as
select brand, 
      category,
      dates.fis_quarter_id,
      count(distinct hshd_id) as tot_hhs
from &sn2._item_store_week dates
inner join &sn2._hh_store_week hh
on dates.fis_week_id=hh.fis_week_id 
and dates.store_id=hh.store_id 
and dates.fis_quarter_id=hh.fis_quarter_id
where category in (&category) and dates.fis_quarter_id in (&fis_quarter_id)
group by brand,category,dates.fis_quarter_id;
EXECUTE dbms_stats.gather_table_stats(ownname=>USER, tabname =>'&final_table',degree=>DBMS_STATS.AUTO_DEGREE);


-------------------------------------------------------------------------------------------------------------------------------




