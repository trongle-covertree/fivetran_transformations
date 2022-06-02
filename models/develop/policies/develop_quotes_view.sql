{{ config(materialized='view') }}

select 
  count_if(EXPOSURES LIKE '%Silver%') as silver,
  count_if(EXPOSURES LIKE '%Gold%') as gold,
  count_if(EXPOSURES LIKE '%Platinum%') as platinum
from develop_policies_policy
where pk like 'POLICY#%' and sk = 'POLICY' and (status = 'Pending-Cancellation' or status = 'Policy-Activated' or status = 'Pending-Esign Required')
