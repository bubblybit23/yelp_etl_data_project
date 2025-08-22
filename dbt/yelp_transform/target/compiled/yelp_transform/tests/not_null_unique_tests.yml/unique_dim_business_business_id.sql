
    
    

select
    business_id as unique_field,
    count(*) as n_records

from yelp_raw_marts.dim_business
where business_id is not null
group by business_id
having count(*) > 1


