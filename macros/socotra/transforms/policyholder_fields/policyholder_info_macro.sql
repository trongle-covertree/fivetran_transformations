{% macro run_socotra_policyholder_info(socotra_db, sf_schema) %}

select
	max(case when field_name = 'first_name' then field_value::varchar end) as first_name,
	max(case when field_name = 'middle_name' then field_value end) as middle_name,
	max(case when field_name = 'last_name' then field_value end) as last_name,
	max(case when field_name = 'email_address' then field_value end) as email_address,
	max(case when field_name = 'primary_contact_number' then field_value end) as primary_contact_number,
	max(case when field_name = 'secondary_contact_number' then field_value end) as secondary_contact_number,
	max(case when field_name = 'mailing_street_address_policyholder' then field_value end) as mailing_street_address_policyholder,
	max(case when field_name = 'mailing_lot_unit_policyholder' then field_value::varchar end) as mailing_lot_unit_policyholder,
	max(case when field_name = 'mailing_city_policyholder' then field_value end) as mailing_city_policyholder,
	max(case when field_name = 'mailing_state_policyholder' then field_value end) as mailing_state_policyholder,
	max(case when field_name = 'mailing_zip_code_policyholder' then field_value end) as mailing_zip_code_policyholder,
	max(case when field_name = 'mailing_county_policyholder' then field_value end) as mailing_county_policyholder,
	max(case when field_name = 'mailing_country_policyholder' then field_value end) as mailing_country_policyholder,
	max(case when field_name = 'type_of_insured' then field_value end) as type_of_insured,
	max(case when field_name = 'third_party_notification_non_consumer' then field_value end) as third_party_notification_non_consumer,
	max(case when field_name = 'rep_first_name' then field_value end) as rep_first_name,
	max(case when field_name = 'organization_name' then field_value end) as organization_name,
	max(case when field_name = 'rep_last_name' then field_value end) as rep_last_name,
	max(case when field_name = 'describe_organization_type' then field_value end) as describe_organization_type,
	max(case when field_name = 'relationship_organization' then field_value end) as relationship_organization,
	max(case when field_name = 'orgainzation_type' then field_value end) as orgainzation_type,
	policyholder_locator,
	to_timestamp_tz(ph.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ph.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.policyholder_fields as phf join
	{{ socotra_db }}.policyholder as ph on ph.locator = phf.policyholder_locator
{% if is_incremental() %}
    where (to_timestamp_tz(ph.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policyholder_info order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ph.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policyholder_info order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}
group by policyholder_locator, ph.datamart_created_timestamp, ph.datamart_updated_timestamp
{% endmacro %}
