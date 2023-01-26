{{ config(materialized='table') }}

select
    pk,
  max(case when name = 'manufactured_home_limit' then to_double(value) end) coverage_a,
  max(case when name = 'other_structures_limit' then to_double(value) end)
  max(case when name = 'unscheduled_personal_property_limit' then to_double(value) end) coverage_c,
  max(case when name = 'loss_of_use_percentage' then to_double((left(value,2))/100) end) coverage_d
from fivetran_covertree.transformations_dynamodb.prod_policy_perils_values_unique_view
group by pk