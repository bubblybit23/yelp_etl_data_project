
    
    

with all_values as (

    select
        avg_rating as value_field,
        count(*) as n_records

    from yelp_raw_marts.dim_business
    group by avg_rating

)

select *
from all_values
where value_field not in (
    '>= 0','<= 5'
)


