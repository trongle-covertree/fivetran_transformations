{% macro run_non_covertree_policy_update(env, prefix) %}

{% set non_covertree_query %}
select property_lead_source, property_communityname, deal_id
from {{ env }}.{{ prefix }}_non_covertree_policies
where deal_id not in (select pk from {{ env }}.{{ prefix }}_policy_exposures_address)
    or deal_id not in (select pk from {{ env }}.{{ prefix }}_policy_characteristics)
    or deal_id not in (select pk from {{ env }}.{{ prefix }}_policy_exposures_grid_info)
{% endset %}

{% set results = run_query(non_covertree_query) %}

{% if execute %}
    {% set property_lead_sources = results.columns[0].values() %}
    {% set property_communitynames = results.columns[1].values() %}
    {% set deal_ids = results.columns[2].values() %}
{% endif %}

{% if property_lead_sources|length > 0 %}

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

    {% set plan_info_merge_query %}
    merge into fivetran_covertree.{{ env }}.{{ prefix }}_policy_exposures_plan_information as pi using (
            select deal_id, name
            from fivetran_covertree.{{ env }}.communities_partner_lookup right outer join fivetran_covertree.{{ env }}.{{ prefix }}_non_covertree_policies
                on lower(trim(name)) = property_communityname) as ncp
        on pi.pk = ncp.deal_id
        when not matched then
            insert (pk, park_name)
            values (ncp.deal_id, name)
    {% endset %}
    {% do run_query(plan_info_merge_query) %}

{% else %}
    SELECT Column1 AS ID
        FROM VALUES
        ('NO FIELDS') limit 0
{% endif %}

{% set cancelled_policy_check %}
select deal_id from fivetran_covertree.hubspot.deal where deal_pipeline_stage_id = '25360262' and deal_id::varchar in (select pk from {{ env }}.{{ prefix }}_policies_policy)
{% endset %}

{% set cancelled_policy_results = run_query(cancelled_policy_check) %}

{% if execute %}
    {% set cancelled_deals = cancelled_policy_results.columns[0].values() %}
{% endif %}

{% if cancelled_deals is defined and cancelled_deals|length == 1 %}
    {% set set_cancelled_policy_queries %}
        UPDATE {{ env }}.{{ prefix }}_policies_policy SET status = 'Policy-Cancelled' where pk in {{ cancelled_deals|replace(",", "") }}
    {% endset %}
    {% do run_query(set_cancelled_policy_queries) %}
{% elif cancelled_deals is defined and cancelled_deals|length > 1 %}
    {% set set_cancelled_policy_queries %}
        UPDATE {{ env }}.{{ prefix }}_policies_policy SET status = 'Policy-Cancelled' where pk in {{ cancelled_deals }}
    {% endset %}
    {% do run_query(set_cancelled_policy_queries) %}
{% endif %}

{% endmacro %}
