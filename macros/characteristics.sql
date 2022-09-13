{% macro run_characteristics(env, prefix) %}

{% set characteristic_query %}
select characteristics, pk
from {{ env }}.{{ prefix }}_policies_policy
{% endset %}

{% set results = run_query(characteristic_query) %}

{% if execute %}
    {% set characteristics = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
{% endif %}


SELECT Column1 AS PK, Column2 AS QUOTE_INCEPTION_DATE, Column3 AS AUTO_POLICY_WITH_AGENCY, Column4 AS DATE_OF_BIRTH,
    Column5 AS REASON_DESCRIPTION, Column6 AS REASON_CODE, Column7 AS PRIOR_CARRIER_NAME, Column8 AS PRIOR_POLICY_EXPIRATION_DATE,
    Column9 AS PRIOR_INSURANCE, Column10 AS ADDITIONALINSURED_DATE_OF_BIRTH, Column11 AS AD_LAST_NAME, Column12 AS AD_FIRST_NAME,
    Column13 AS RELATIONSHIP_TO_POLICYHOLDER, Column14 AS CLAIM_AMOUNT, Column15 AS CLAIM_NUMBER, Column16 AS DESCRIPTION_OF_LOSS,
    Column17 AS CLAIM_CAT, Column18 AS CLAIM_SOURCE, Column19 AS CATEGORY, Column20 AS CLAIM_DATE, Column21 AS COUNTRY,
    Column22 AS AGENCY_PHONE_NUMBER, Column23 AS EMAIL_ADDRESS, Column24 AS CITY, Column25 AS AGENT_ID, Column26 AS LOT_UNIT,
    Column27 AS AGENCY_ID, Column28 AS AGENCY_CONTACT_NAME, Column29 AS STATE, Column30 AS AGENCY_LICENSE, Column31 AS STREET_ADDRESS,
    Column32 AS ZIP_CODE, Column33 AS ANIMAL_BITE, Column34 AS CONVICTION, Column35 AS CANCELLATION_RENEW,
    Column36 AS PREVIOUS_STREET_ADDRESS_POLICYHOLDER, Column37 AS PREVIOUS_COUNTRY_POLICYHOLDER, Column38 AS PREVIOUS_ZIP_CODE_POLICYHOLDER,
    Column39 AS PREVIOUS_CITY_POLICYHOLDER, Column40 AS PREVIOUS_LOT_UNIT_POLICYHOLDER, Column41 AS PREVIOUS_STATE_POLICYHOLDER,
    Column42 AS ASSOCIATION_DISCOUNT, Column43 AS PAPERLESS_DISCOUNT, Column44 AS MULTI_POLICY_DISCOUNT, Column45 AS APPLICATION_INTIATION,
    Column46 AS INSURANCE_SCORE, Column47 AS GROSS_PREMIUM, Column48 AS GROSS_PREMIUM_CURRENCY, Column49 AS GROSS_TAXES, Column50 AS GROSS_TAXES_CURRENCY,
    to_timestamp(Column51) AS CREATED_TIMESTAMP, to_timestamp(Column52) AS UPDATED_TIMESTAMP FROM VALUES
{% for char in characteristics %}
    {% set outer_loop = loop %}
    {% if char %}
        {% for char_json in fromjson(char) %}
{# {{ log(mod, info=True) }} #}
(
    '{{ pk[loop.index0] }}',
            {% for field_group_key in char_json['fieldGroupsByLocator'].keys() %}
    {% if 'quote_inception_date' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['quote_inception_date'][0] }}'{% else %}null{% endif %},
    {% if 'auto_policy_with_agency' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['auto_policy_with_agency'][0] }}'{% else %}null{% endif %},
    {% if 'date_of_birth' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['date_of_birth'][0] }}'{% else %}null{% endif %},
    {% if 'reason_description' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['reason_description'][0] }}'{% else %}null{% endif %},
    {% if 'reason_code' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['reason_code'][0] }}'{% else %}null{% endif %},
    {% if 'prior_carrier_name' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['prior_carrier_name'][0] }}'{% else %}null{% endif %},
    {% if 'prior_policy_expiration_date' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['prior_policy_expiration_date'][0] }}'{% else %}null{% endif %},
    {% if 'prior_insurance' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['prior_insurance'][0] }}'{% else %}null{% endif %},
    {% if 'additionalinsured_date_of_birth' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['additionalinsured_date_of_birth'][0] }}'{% else %}null{% endif %},
    {% if 'ad_last_name' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['ad_last_name'][0] }}'{% else %}null{% endif %},
    {% if 'ad_first_name' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['ad_first_name'][0] }}'{% else %}null{% endif %},
    {% if 'relationship_to_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['relationship_to_policyholder'][0] }}'{% else %}null{% endif %},
    {% if 'claim_amount' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['claim_amount'][0] }}'{% else %}null{% endif %},
    {% if 'claim_number' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['claim_number'][0] }}'{% else %}null{% endif %},
    {% if 'description_of_loss' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['description_of_loss'][0] }}'{% else %}null{% endif %},
    {% if 'claim_cat' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['claim_cat'][0] }}'{% else %}null{% endif %},
    {% if 'claim_source' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['claim_source'][0] }}'{% else %}null{% endif %},
    {% if 'category' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['category'][0] }}'{% else %}null{% endif %},
    {% if 'claim_date' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['claim_date'][0] }}'{% else %}null{% endif %},
    {% if 'country' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['country'][0] }}'{% else %}null{% endif %},
    {% if 'agency_phone_number' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['agency_phone_number'][0] }}'{% else %}null{% endif %},
    {% if 'email_address' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['email_address'][0] }}'{% else %}null{% endif %},
    {% if 'city' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['city'][0] }}'{% else %}null{% endif %},
    {% if 'agent_id' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['agent_id'][0] }}'{% else %}null{% endif %},
    {% if 'lot_unit' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['lot_unit'][0] }}'{% else %}null{% endif %},
    {% if 'agency_id' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['agency_id'][0] }}'{% else %}null{% endif %},
    {% if 'agency_contact_name' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['agency_contact_name'][0] }}'{% else %}null{% endif %},
    {% if 'state' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['state'][0] }}'{% else %}null{% endif %},
    {% if 'agency_license' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['agency_license'][0] }}'{% else %}null{% endif %},
    {% if 'street_address' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['street_address'][0] }}'{% else %}null{% endif %},
    {% if 'zip_code' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['zip_code'][0] }}'{% else %}null{% endif %},
    {% if 'animal_bite' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['animal_bite'] [0]}}'{% else %}null{% endif %},
    {% if 'conviction' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['conviction'][0] }}'{% else %}null{% endif %},
    {% if 'cancellation_renew' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['cancellation_renew'][0] }}'{% else %}null{% endif %},
    {% if 'previous_street_address_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['previous_street_address_policyholder'][0] }}'{% else %}null{% endif %},
    {% if 'previous_country_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['previous_country_policyholder'][0] }}'{% else %}null{% endif %},
    {% if 'previous_zip_code_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['previous_zip_code_policyholder'][0] }}'{% else %}null{% endif %},
    {% if 'previous_city_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['previous_city_policyholder'][0] }}'{% else %}null{% endif %},
    {% if 'previous_lot_unit_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['previous_lot_unit_policyholder'][0] }}'{% else %}null{% endif %},
    {% if 'previous_state_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}'{{ char_json['fieldGroupsByLocator'][field_group_key]['previous_state_policyholder'][0] }}'{% else %}null{% endif %},
            {% endfor %}
    {% if 'fieldValues' in char_json and 'association_discount' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['association_discount'][0] }}'{% else %}null{% endif %},
    {% if 'fieldValues' in char_json and 'paperless_discount' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['paperless_discount'][0] }}'{% else %}null{% endif %},
    {% if 'fieldValues' in char_json and 'multi_policy_discount' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['multi_policy_discount'][0] }}'{% else %}null{% endif %},
    {% if 'fieldValues' in char_json and 'application_intiation' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['application_intiation'][0] }}'{% else %}null{% endif %},
    {% if 'fieldValues' in char_json and 'insurance_score' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['insurance_score'][0] }}'{% else %}null{% endif %},
    '{{ char_json.grossPremium or null }}',
    '{{ char_json.grossPremiumCurrency or null }}',
    '{{ char_json.grossTaxes or null }}',
    '{{ char_json.grossTaxesCurrency or null }}',
    '{{ char_json.createdTimestamp or null }}',
    '{{ char_json.updatedTimestamp or null }}'
){% if not outer_loop.last or not loop.last %},{% endif %}
        {% endfor %}
    {% endif %}
{% endfor %}
{% endmacro %}