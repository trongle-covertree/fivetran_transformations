{% macro run_non_covertree_policy_update(env, prefix) %}

{% set non_covertree_query %}
select property_lead_source, property_communityname, deal_id
from {{ env }}.{{ prefix }}_non_covertree_policies
where deal_id not in (select pk from {{ env }}.{{ prefix }}_policies_policy)
{% endset %}

{% set results = run_query(non_covertree_query) %}

{% if execute %}
    {% set property_lead_sources = results.columns[0].values() %}
    {% set property_communitynames = results.columns[1].values() %}
    {% set deal_ids = results.columns[2].values() %}
{% endif %}

{% if property_lead_sources|length > 0 %}
{% set policy_merge_query %}
merge into fivetran_covertree.{{ env }}.{{ prefix }}_policies_policy as p using fivetran_covertree.{{ env }}.{{ prefix }}_non_covertree_policies as ncp
    on p.pk = ncp.deal_id
    when not matched then
        insert (pk, created_timestamp, issued_timestamp, effective_contract_end_timestamp, locator, status)
        values (ncp.deal_id, ncp.property_createdate, ncp.property_policy_effective_date, ncp.property_policy_end_date, property_policy_number,
            iff(ncp.property_policy_end_date > current_timestamp(), 'Policy-Activated', 'Policy-Cancelled'))
{% endset %}
{% do run_query(policy_merge_query) %}

{% set address_merge_query %}
merge into fivetran_covertree.{{ env }}.{{ prefix }}_policy_exposures_address as a using fivetran_covertree.{{ env }}.{{ prefix }}_non_covertree_policies as ncp
    on a.pk = ncp.deal_id
    when not matched then
        insert (pk, created_timestamp, policy_created_timestamp, street_address, city, state, zip_code, country)
        values (ncp.deal_id, ncp.property_createdate, ncp.property_createdate, ncp.property_address, ncp.property_city, ncp.property_state, ncp.property_zip, 'USA')
{% endset %}
{% do run_query(address_merge_query) %}

{% set char_merge_query %}
merge into fivetran_covertree.{{ env }}.{{ prefix }}_policy_characteristics as c using fivetran_covertree.{{ env }}.{{ prefix }}_non_covertree_policies as ncp
    on c.pk = ncp.deal_id
    when not matched then
        insert (pk, created_timestamp, gross_premium)
        values (ncp.deal_id, ncp.property_createdate, ncp.property_amount)
{% endset %}
{% do run_query(char_merge_query) %}

{% set grid_info_merge_query %}
merge into fivetran_covertree.{{ env }}.{{ prefix }}_policy_exposures_grid_info as g using (
        select deal_id, iff(ct_mhcid is null and contains(lower(trim(property_lead_source)), 'roots'), '999999999999999999', ct_mhcid) as ct_mhcid
        from fivetran_covertree.{{ env }}.communities_partner_lookup right outer join fivetran_covertree.{{ env }}.{{ prefix }}_non_covertree_policies
            on lower(trim(name)) = property_communityname) as ncp
    on g.pk = ncp.deal_id
    when not matched then
        insert (pk, ct_mhcid)
        values (ncp.deal_id, ncp.ct_mhcid)
{% endset %}
{% do run_query(grid_info_merge_query) %}

{% else %}
SELECT Column1 AS ID
    FROM VALUES
    ('NO FIELDS') limit 0
{% endif %}
{% endmacro %}