{{ config(materialized='table') }}

select p.pk, p.name, iff(name = 'loss_of_use_percentage', to_double(left(value, 2))/100, value) as value from prod_policy_perils_values_unique_view as p inner join prod_policy_plan_information_unique_view as pi on pi.pk = p.pk where name in ('manufactured_home_limit', 'unscheduled_personal_property_limit', 'loss_of_use_percentage')