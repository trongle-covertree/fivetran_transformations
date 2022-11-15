{% macro run_referrals(env, prefix, partners_db) %}

{% set referrals_query %}
select pk, policy_locator, created_at
{# {{ log(pk[loop.index0], info=True) }} #}
from {{ env }}.{{ prefix }}_leads
where pk like 'FRIENDBUY#%' and policy_locator is not null
{% if is_incremental() %}
    where (created_at > (select CREATED_TIMESTAMP from {{ env }}.{{ prefix }}_policies_referrals order by CREATED_TIMESTAMP desc limit 1))
        or policy_locator not in (select policy_locator from {{ env }}.{{ prefix }}_policies_referrals)
{% endif %}
{% endset %}

{% set results = run_query(referrals_query) %}

{% if execute %}
    {% set pks = results.columns[0].values() %}
    {% set policy_locators = results.columns[1].values() %}
    {% set created_timestamps = results.columns[2].values() %}
{% endif %}

{% set hubspot_manual_referrals_query %}
select pk, referrer_communities, locator
from {{ env }}.hubspot_referrals
where locator not in (select policy_locator from {{ env }}.{{ prefix }}_leads where policy_locator is not null and pk like 'FRIENDBUY%')
{% if is_incremental() %}
    and locator not in (select policy_locator from {{ env }}.{{ prefix }}_policies_referrals)
{% endif %}
{% endset %}

{% set hubspot_manual_referrals_results = run_query(hubspot_manual_referrals_query)%}

{% if execute %}
    {% set hubspot_manual_referrals_pk = hubspot_manual_referrals_results.columns[0].values() %}
    {% set hubspot_manual_referrals_communities = hubspot_manual_referrals_results.columns[1].values() %}
    {% set hubspot_manual_referrals_locator = hubspot_manual_referrals_results.columns[2].values() %}
{% endif %}

{% set partners_query %}
select pk, community_name, community_manager_name, community_manager_email, partner_name
from {{ partners_db }}.{{ prefix }}_partners_table
where pk like 'FRIENDBUY#%' and _fivetran_deleted = false
{% endset %}

{% set partners_results = run_query(partners_query)%}

{% if execute %}
    {% set partners_pk = partners_results.columns[0].values() %}
    {% set partners_community_name = partners_results.columns[1].values() %}
    {% set partners_community_manager_name = partners_results.columns[2].values() %}
    {% set partners_community_manager_email = partners_results.columns[3].values() %}
    {% set partners_name = partners_results.columns[4].values() %}
{% endif %}

SELECT Column1 AS PK, Column2 AS  POLICY_LOCATOR, Column3 AS partner_name, Column4 AS community_manager_email, Column5 AS community_manager_name, Column6 AS community_name,
    Column7 AS friendbuy_pk, Column8 AS CREATED_TIMESTAMP
    FROM VALUES

{% if hubspot_manual_referrals_pk|length > 0 %}
    {% if is_incremental() %}
        {% if hubspot_manual_referrals_pk|length == 1 %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policies_referrals where policy_locator in {{ hubspot_manual_referrals_locator|replace(",", "") }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% else %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policies_referrals where policy_locator in {{ hubspot_manual_referrals_locator }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% endif %}
    {% endif %}

    {% for referral_community_name in hubspot_manual_referrals_communities %}
        {% set outer_loop = loop %}
        {% for partner_community_name in partners_community_name %}
            {% if referral_community_name == partner_community_name %}
                (
                    '{{ hubspot_manual_referrals_pk[outer_loop.index0] }}',
                    '{{ hubspot_manual_referrals_locator[outer_loop.index0] }}',
                    '{{ partners_name[loop.index0] }}',
                    '{{ partners_community_manager_email[loop.index0] }}',
                    '{{ partners_community_manager_name[loop.index0] | replace("'", "\\'") }}',
                    '{{ partners_community_name[loop.index0] | replace("'", "\\'") }}',
                    '{{ partners_pk[loop.index0] }}',
                    {{ dbt_date.now("America/New_York") }}
                ){% if not outer_loop.last or (outer_loop.last and pk|length > 0) %},{% endif %}
            {% endif %}
        {% endfor %}
    {% endfor %}
{% endif %}

{% if pks|length > 0 %}
    {% if is_incremental() %}
        {% if pks|length == 1 %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policies_referrals where policy_locator in {{ policy_locators|replace(",", "") }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% else %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policies_referrals where policy_locator in {{ policy_locators }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% endif %}
    {% endif %}

    {% for pk in pks %}
        {% set outer_loop = loop %}
        {% for partner_pk in partners_pk %}
            {% if pk == partner_pk %}
                (
                    'POLICY#{{ policy_locators[outer_loop.index0] }}',
                    '{{ policy_locators[outer_loop.index0] }}',
                    '{{ partners_name[loop.index0] }}',
                    '{{ partners_community_manager_email[loop.index0] }}',
                    '{{ partners_community_manager_name[loop.index0] | replace("'", "\\'") }}',
                    '{{ partners_community_name[loop.index0] | replace("'", "\\'") }}',
                    '{{ partner_pk }}',
                    '{{ created_timestamps[outer_loop.index0] }}'
                ){% if not outer_loop.last %},{% endif %}
            {% endif %}
        {% endfor %}
    {% endfor %}
{% endif %}

{% if pk|length == 0 and hubspot_manual_referrals_pk|length == 0 %}
    (null, null, null, null, null, null, null, null) limit 0
{% endif %}
{% endmacro %}