DECLARE @final_yr int 
DECLARE @curr_yr int
SELECT @final_yr = max(ROLL_YR - 1) from ASSESSMENT
SELECT @curr_yr = max(ROLL_YR) from ASSESSMENT

select p.swis,
section,
sub_section,
block,
lot,
sub_lot,
suffix,
print_key,
grid_east,
grid_north,
loc_prefix_dir,
loc_st_nbr,
loc_st_name, 
loc_mail_st_suff,
loc_post_dir,
loc_unit_name,
loc_unit_nbr,
loc_muni_name,
loc_zip,'' as loc_zip4,
coalesce(account_nbr,' ') as account_number,
convert(char(8),parcel_create_date,112) as parcel_create_date,
active,
a.roll_yr,
a.prop_class,
a.roll_section,
p.parcel_id,
convert(char(8),fmdate,112) as fmdate,
convert(char(8),sale_date,112) as sale_date,
a.own_code,
powner_id,
o.owner_type,
o.owner_first_name,
o.owner_initial_name,
o.owner_last_name,
o.owner_name_suffix,
secondary_name,
prefix_dir,
mail_st_nbr,
mail_st_rt,
own_mail_st_suff,
post_dir,
mail_city,
own_mail_state,
mail_country,
mail_zip,
po_box,
own_unit_name,
own_unit_nbr,
owner_addl_addr,
desig_status,
coalesce(pa.owner_type,' ') as owner_typ,
coalesce(pa.owner_id,0) as pa_owner_id,
coalesce(pa.owner_first_name,' ') as pa_owner_first_name,
coalesce(pa.owner_initial_name,' ') as pa_owner_init_name,
coalesce(pa.owner_last_name,' ') as pa_owner_last_name,
coalesce(pa.owner_name_suffix,' ') as pa_owner_name_suffix,
coalesce(pr.own_code,' ') as own_code
from
(select swis,section, sub_section,block,lot,sub_lot,suffix,print_key,grid_east,
grid_north,loc_prefix_dir,loc_st_nbr,loc_st_name,
loc_mail_st_suff,loc_post_dir,loc_unit_name,loc_unit_nbr,loc_muni_name,
loc_zip,account_nbr,parcel_create_date,owner_id,parcel_id
from parcel) as p
left outer join
(select swis,parcel_id,active,roll_yr,prop_class,roll_section,bank_code,
mortgage_nbr,sch_code,own_code,land_av,total_av,descript_1,
descript_2,descript_3
from assessment 
where roll_yr = @final_yr) as a
on a.swis = p.swis
and a.parcel_id = p.parcel_id
left outer join 
(select date(last_modified) as fmdate, parcel_id,swis from track_mgr) as t
on t.swis = p.swis
and t.parcel_id = p.parcel_id 
left outer join
(select z.owner_id as powner_id,owner_type,owner_first_name,
owner_initial_name,owner_last_name,owner_name_suffix,secondary_name,
prefix_dir, mail_st_nbr,mail_st_rt,own_mail_st_suff,post_dir,
mail_city,own_mail_state,mail_country,mail_zip,po_box,own_unit_name,
own_unit_nbr, owner_addl_addr,desig_status,z.swis as pswis,z.parcel_id as pparcel_id, 
date(sale_date) as sale_date
from owner z, parcel_to_owner x
where owner_type = 'P'
and ranking_by_roll_yr = 1
and roll_yr = @curr_yr
and pswis = x.swis
and pparcel_id = x.parcel_id
and powner_id = x.owner_id) as o
on o.pswis    = p.swis
and o.pparcel_id = p.parcel_id
left outer join
(select min(q.owner_id) as ownera_id,q.parcel_id,q.swis 
from owner q, parcel_to_owner r 
where owner_type = 'A'
and ranking_by_roll_yr = 1
and q.swis = r.swis
and q.parcel_id = r.parcel_id
and q.owner_id  = r.owner_id
and r.roll_yr = @curr_yr
group by q.swis,q.parcel_id)as qa
on qa.swis = p.swis
and qa.parcel_id = p.parcel_id
left outer join
(select owner_type,owner_id,owner_first_name,owner_initial_name,owner_last_name,
owner_name_suffix,swis,parcel_id 
from owner x 
where owner_type = 'A') as pa
on pa.owner_id = qa.ownera_id
and pa.swis = qa.swis
and pa.parcel_id = qa.parcel_id
left outer join
(select front,depth,acres,swis,parcel_id 
from land
where site_nbr = 0
and roll_yr = @curr_yr) as l
on l.swis = p.swis
and l.parcel_id = p.parcel_id
left outer join
(select swis,parcel_id,pcl_book as book,pcl_page as page from historical h
where h.COUNTER = (select max(counter) from historical x where h.swis = x.swis and h.parcel_id = x.parcel_id)
and book > ' '
union
select swis,parcel_id,book,page
from deed_ref d
where counter = (select max(counter)from deed_ref y 
where d.swis = y.swis and d.parcel_id = y.parcel_id)
and book > ' ') as dd
on dd.swis = p.swis
and dd.parcel_id = p.parcel_id
left outer join
(select sewer_type, water_supply,swis,parcel_id 
 from site
 where site_nbr = 1
 and roll_yr = @curr_yr) as s
on s.parcel_id = p.parcel_id
and s.swis = p.swis
left outer join
(select bldg_style,nbr_bedrooms,swis,parcel_id
from res_bldg
where roll_yr = @curr_yr
and site_nbr = 1)as r
on r.parcel_ID = p.parcel_id
and r.swis = p.swis
left outer join
(select sch_code,roll_section,land_av,total_av,prop_class,
own_code,swis,parcel_id
from assessment
where roll_yr = @final_yr) as pr
on pr.parcel_id = p.parcel_id
and pr.swis = p.swis
GROUP BY p.swis,section,sub_section,block,lot,sub_lot,suffix,print_key,
grid_east,grid_north,loc_prefix_dir,loc_st_nbr,loc_st_name, 
loc_mail_st_suff,loc_post_dir,loc_unit_name,loc_unit_nbr,loc_muni_name,
loc_zip,account_nbr,parcel_create_date,active,a.roll_yr,a.prop_class,
a.roll_section,bank_code,mortgage_nbr,a.sch_code,coalesce(front,0),
coalesce(depth,0),coalesce(acres,0),p.parcel_id,fmdate,sale_date,
book,page,a.own_code,a.land_av,a.total_av,descript_1,descript_2,
descript_3, powner_id,o.owner_type,o.owner_first_name,
o.owner_initial_name,o.owner_last_name,o.owner_name_suffix,secondary_name,
prefix_dir, mail_st_nbr,mail_st_rt,own_mail_st_suff,post_dir,mail_city,
own_mail_state,mail_country,mail_zip,po_box,own_unit_name,own_unit_nbr,
owner_addl_addr,desig_status,coalesce(pa.owner_type,' '),
coalesce(pa.owner_id,0),coalesce(pa.owner_first_name,' '),
coalesce(pa.owner_initial_name,' '),coalesce(pa.owner_last_name,' '),
coalesce(pa.owner_name_suffix,' '),
coalesce(bldg_style,' '),coalesce(sewer_type,' '),coalesce(nbr_bedrooms,0),
coalesce(water_supply,' '),coalesce(pr.sch_code,' '),
coalesce(pr.roll_section,' '),coalesce(pr.land_av,0),coalesce(pr.total_av,0),
coalesce(pr.prop_class,' '),coalesce(pr.own_code,' ')
order by p.swis,print_key