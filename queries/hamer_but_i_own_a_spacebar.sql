DECLARE @final_yr int 
DECLARE @curr_yr int
SELECT @final_yr = max(ROLL_YR - 1) from ASSESSMENT
SELECT @curr_yr = max(ROLL_YR) from ASSESSMENT

SELECT p.swis,
section,
sub_section,
block,
lot,sub_lot,
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
loc_zip,
'' as loc_zip4,
coalesce(account_nbr,' ') as col_account_nbr,
convert(char(8),parcel_create_date,112),
active,
a.roll_yr,
a.prop_class,
a.roll_section,
bank_code,
mortgage_nbr,
a.sch_code,coalesce(front,0),
coalesce(depth,0),
coalesce(acres,0),
p.parcel_id,
convert(char(8),fmdate,112),
convert(char(8),sale_date,112),
book,
page,
a.own_code,
a.land_av,
a.total_av,
descript_1,
descript_2,
descript_3,
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
desig_status,coalesce(pa.owner_type,' '),
coalesce(pa.owner_id,0),
coalesce(pa.owner_first_name,' '),
coalesce(pa.owner_initial_name,' '),
coalesce(pa.owner_last_name,' '),
coalesce(pa.owner_name_suffix,' '),
coalesce(bldg_style,' '),
coalesce(sewer_type,' '),
coalesce(nbr_bedrooms,0),
coalesce(water_supply,' '),
coalesce(pr.sch_code,' '),
coalesce(pr.roll_section,' '),
coalesce(pr.land_av,0),
coalesce(pr.total_av,0),
coalesce(pr.prop_class,' '),
coalesce(pr.own_code,' ')

FROM parcel p

LEFT JOIN asssessment a
WHERE a.roll_yr = @final_yr
ON a.swis = p.swis
AND a.parcel_id = p.parcel_id

LEFT JOIN
(SELECT DATE(last_modified) AS fmdate, parcel_id, swis from track_mgr) AS t
ON t.swis = p.swis
AND t.parcel_id = p.parcel_id 

LEFT JOIN
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
and powner_id = x.owner_id) o

ON o.pswis = p.swis
AND o.pparcel_id = p.parcel_id

LEFT JOIN
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

LEFT JOIN
(select owner_type,owner_id,owner_first_name,owner_initial_name,owner_last_name,
owner_name_suffix,swis,parcel_id 
from owner x 
where owner_type = 'A') as pa
on pa.owner_id = qa.ownera_id
and pa.swis = qa.swis
and pa.parcel_id = qa.parcel_id

