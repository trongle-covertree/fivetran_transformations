{{ config(materialized='view') }}

select 
  sum(case when EXPOSURES LIKE '%Coverage A - Dwelling%' then 1 end) as "Coverage A",
  sum(case when EXPOSURES LIKE '%Coverage B - Other Structures%' then 1 end) as "Coverage B",
  sum(case when EXPOSURES LIKE '%Coverage C - Personal Property%' then 1 end) as "Coverage C",
  sum(case when EXPOSURES LIKE '%Coverage D - Loss of Use%' then 1 end) as "Coverage D",
  sum(case when EXPOSURES LIKE '%Coverage E - Personal Liability%' then 1 end) as "Coverage E",
  sum(case when EXPOSURES LIKE '%Coverage F - Medical Payment to Others%' then 1 end) as "Coverage F",
  sum(case when EXPOSURES LIKE '%Deductibles%' then 1 end) as "Deductibles",
  sum(case when EXPOSURES LIKE '%Trip Collision%' then 1 end) as "Trip Collision",
  sum(case when EXPOSURES LIKE '%Identity Fraud Expense%' then 1 end) as "Identity Fraud Expense",
  sum(case when EXPOSURES LIKE '%Golf Cart%' then 1 end) as "Golf Cart",
  sum(case when EXPOSURES LIKE '%Enhanced Coverage%' then 1 end) as "Enhanced Coverage",
  sum(case when EXPOSURES LIKE '%Earthquake Coverage%' then 1 end) as "Earthquake Coverage",
  sum(case when EXPOSURES LIKE '%Vacation Rental Coverage%' then 1 end) as "Vacation Rental Coverage",
  sum(case when EXPOSURES LIKE '%Water Backup and Sump Overflow%' then 1 end) as "Water Backup and Sump Overflow",
  sum(case when EXPOSURES LIKE '%Inflation Guard%' then 1 end) as "Inflation Guard",
  sum(case when EXPOSURES LIKE '%Hobby - Incidental Farming%' then 1 end) as "Hobby - Incidental Farming",
  sum(case when EXPOSURES LIKE '%Loss Assessment%' then 1 end) as "Loss Assessment",
  sum(case when EXPOSURES LIKE '%Increased Debris Removal%' then 1 end) as "Increased Debris Removal",
  sum(case when EXPOSURES LIKE '%Scheduled Personal Property%' then 1 end) as "Scheduled Personal Property"
from {{ ref('policies_policy')}}
where pk like 'POLICY#%' and sk = 'POLICY' and (status = 'Pending-Cancellation' or status = 'Policy-Activated' or status = 'Pending-Esign Required')