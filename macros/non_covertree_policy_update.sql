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

{% set grid_info_check %}
select count(deal_id)
from {{ env }}.{{ prefix }}_non_covertree_policies
where deal_id not in (select pk from {{ env }}.{{ prefix }}_policy_exposures_grid_info)
{% endset %}

{% set results = run_query(grid_info_check) %}

{% if execute %}
    {% set deal_id_length = results.columns[2].values() %}
{% endif %}

    {% if deal_id_length == 1 %}
        {% set community_partners_query %}
            select ct_mhcid, deal_id, ncp.property_lead_source
            from {{ env }}.communities_partner_lookup as c right outer join {{ env }}.{{ prefix }}_non_covertree_policies as ncp
            on lower(trim(c.name)) = ncp.property_communityname where deal_id in {{ deal_ids|replace(",", "") }}
        {% endset %}

        {% set community_partners_results = run_query(community_partners_query) %}

        {% if execute %}
            {% set ct_mhcids = community_partners_results.columns[0].values() %}
            {% set grid_info_deal_ids = community_partners_results.columns[1].values() %}
            {% set grid_lead_sources = community_partners_results.columns[2].values() %}
        {% endif %}
        INSERT INTO fivetran_covertree.{{ env }}.{{ prefix }}_policy_exposures_grid_info (pk, ct_mhcid) VALUES
            (
                {% if grid_info_deal_ids[0] is not none or grid_info_deal_ids[0]|length > 0 %}'{{ grid_info_deal_ids[0] }}'{% else %}null{% endif %},
                {% if ct_mhcids[0] is not none or ct_mhcids[0]|length > 0 %}'{{ ct_mhcids[0] }}'{% elif 'roots' in grid_lead_sources[0]|lower %}'999999999999999999'{% else %}null{% endif %}
            )
    {% else %}
        {% set community_partners_query %}
            select ct_mhcid, deal_id, ncp.property_lead_source
            from {{ env }}.communities_partner_lookup as c right outer join {{ env }}.{{ prefix }}_non_covertree_policies as ncp
            on lower(trim(c.name)) = ncp.property_communityname where deal_id in {{ deal_ids }}
        {% endset %}

        {% set community_partners_results = run_query(community_partners_query) %}

        {% if execute %}
            {% set ct_mhcids = community_partners_results.columns[0].values() %}
            {% set grid_info_deal_ids = community_partners_results.columns[1].values() %}
            {% set grid_lead_sources = community_partners_results.columns[2].values() %}
        {% endif %}
            INSERT INTO fivetran_covertree.{{ env }}.{{ prefix }}_policy_exposures_grid_info (pk, ct_mhcid) VALUES
        {% for ct_id in ct_mhcids %}
            (
                {% if grid_info_deal_ids[loop.index0] is not none and grid_info_deal_ids[loop.index0]|length > 0 %}'{{ grid_info_deal_ids[loop.index0] }}'{% else %}null{% endif %},
                {% if ct_id is not none and ct_id|length > 0 %}'{{ ct_id }}'{% elif 'roots' in grid_lead_sources[loop.index0]|lower %}'999999999999999999'{% else %}null{% endif %}
            ){% if not loop.last %},{% endif %}
        {% endfor %}
    {% endif %}
{% else %}
SELECT Column1 AS ID
    FROM VALUES
    ('NO FIELDS') limit 0
{% endif %}
{% endmacro %}