{{ config(materialized='view') }}

select 
  sum(case when EXPOSURES LIKE '%Silver%' then 1 end) as Silver,
  sum(case when EXPOSURES LIKE '%Gold%' then 1 end) as Gold,
  sum(case when EXPOSURES LIKE '%Platinum%' then 1 end) as Platinum
from policies_policy
where pk like 'POLICY#%' and sk = 'POLICY' and (status = 'Pending-Cancellation' or status = 'Policy-Activated' or status = 'Pending-Esign Required')