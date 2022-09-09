{% macro run_policyholders(env, prefix, locator_type ) %}

{% set policyholder_modern_query %}
select entity, pk
from {{ env }}.{{ prefix }}_policyholders_person_locator
where SK = {{ locator_type }} and entity like '%email_address%'

{% endset %}

{% set results = run_query(policyholder_modern_query) %}

{% if execute %}
    {% set modern_entities = results.columns[0].values() %}
    {% set modern_pk = results.columns[1].values() %}
{% endif %}

{% set policyholder_legacy_query %}
select entity, pk
from {{ env }}.{{ prefix }}_policyholders_person_locator
where SK = {{ locator_type }} and entity like '%emailAddress%'
{% endset %}

{% set results_legacy = run_query(policyholder_legacy_query)%}

{% if execute %}
    {% set legacy_entities = results_legacy.columns[0].values() %}
    {% set legacy_pk = results_legacy.columns[1].values() %}
{% endif %}


SELECT Column1 as PK, Column2 as ACCOUNT_LOCATOR, Column3 as COMPLETED, to_timestamp(Column4) as CREATED_TIMESTAMP, parse_json(column5) as FLAGS,
    Column6 as LOCATOR, Column7 as REVISION, to_timestamp(Column8) as UPDATED_TIMESTAMP, Column9 as EMAIL_ADDRESS, Column10 as FIRST_NAME,
     Column11 as LAST_NAME, Column12 as MAILING_CITY_POLICYHOLDER, Column13 as MAILING_COUNTRY_POLICYHOLDER, Column14 as MAILING_COUNTY_POLICYHOLDER,
     Column15 as MAILING_LOT_UNIT_POLICYHOLDER, Column16 as MAILING_STATE_POLICYHOLDER, Column17 as MAILING_STREET_ADDRESS_POLICYHOLDER,
     Column18 as MAILING_ZIP_CODE_POLICYHOLDER, Column19 as MIDDLE_NAME, Column20 as PRIMARY_CONTACT_NUMBER,
      Column21 as THIRD_PARTY_NOTIFICATION_NON_CONSUMER, Column22 as TYPE_OF_INSURED, Column23 as ORGANIZATION_TYPE, Column24 as ORGANIZATION_NAME,
      Column25 as POLICYHOLDER_ID, Column26 as RELATIONSHIP_ORGANIZATION, Column27 as REP_FIRST_NAME, Column28 as REP_LAST_NAME
FROM VALUES 
{% for entity in modern_entities %}
    {% if entity %}
        {% set entity_json = fromjson(entity) %}
{# {{ log(entity_json['values']['email_address'][0], info=True) }} #}
(
    '{{ modern_pk[loop.index0] }}',
    '{{ entity_json.accountLocator or null }}',
    {{ entity_json.completed }},
    '{{ entity_json.createdTimestamp }}',
    '{{ tojson(entity_json.flags) }}',
    '{{ entity_json.locator }}',
    '{{ entity_json.revision }}',
    '{{ entity_json.updatedTimestamp }}',
    {% if 'values' in entity_json and 'email_address' in entity_json['values'] %}'{{ entity_json['values']['email_address'][0] | replace("'", "\\'") }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'first_name' in entity_json['values'] %}'{{ entity_json['values']['first_name'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'last_name' in entity_json['values'] %}'{{ entity_json['values']['last_name'][0] | replace("'", "\\'") }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailing_city_policyholder' in entity_json['values'] %}'{{ entity_json['values']['mailing_city_policyholder'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailing_country_policyholder' in entity_json['values'] %}'{{ entity_json['values']['mailing_country_policyholder'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailing_county_policyholder' in entity_json['values'] %}'{{ entity_json['values']['mailing_county_policyholder'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailing_lot_unit_policyholder' in entity_json['values'] %}'{{ entity_json['values']['mailing_lot_unit_policyholder'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailing_state_policyholder' in entity_json['values'] %}'{{ entity_json['values']['mailing_state_policyholder'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailing_street_address_policyholder' in entity_json['values'] %}'{{ entity_json['values']['mailing_street_address_policyholder'][0] | replace("'", "\\'") }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailing_zip_code_policyholder' in entity_json['values'] %}'{{ entity_json['values']['mailing_zip_code_policyholder'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'middle_name' in entity_json['values'] %}'{{ entity_json['values']['middle_name'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'primary_contact_number' in entity_json['values'] %}'{{ entity_json['values']['primary_contact_number'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'third_party_notification_non_consumer' in entity_json['values'] %}'{{ entity_json['values']['third_party_notification_non_consumer'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'type_of_insured' in entity_json['values'] %}'{{ entity_json['values']['type_of_insured'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'organization_type' in entity_json['values'] %}'{{ entity_json['values']['organization_type'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'organization_name' in entity_json['values'] %}'{{ entity_json['values']['organization_name'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'policyholder_id' in entity_json['values'] %}'{{ entity_json['values']['policyholder_id'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'relationship_organization' in entity_json['values'] %}'{{ entity_json['values']['relationship_organization'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'rep_first_name' in entity_json['values'] %}'{{ entity_json['values']['rep_first_name'][0] }}',{% else %}null,{% endif %}
    {% if 'values' in entity_json and 'rep_last_name' in entity_json['values'] %}'{{ entity_json['values']['rep_last_name'][0] }}'{% else %}null{% endif %}
){% if not loop.last %},{% endif %}
    {% endif %}
{% endfor %}
UNION ALL

SELECT Column1 as PK, Column2 as ACCOUNT_LOCATOR, Column3 as COMPLETED, to_timestamp(Column4) as CREATED_TIMESTAMP, parse_json(column5) as FLAGS,
    Column6 as LOCATOR, Column7 as REVISION, to_timestamp(Column8) as UPDATED_TIMESTAMP, Column9 as EMAIL_ADDRESS, Column10 as FIRST_NAME,
     Column11 as LAST_NAME, Column12 as MAILING_CITY_POLICYHOLDER, Column13 as MAILING_COUNTRY_POLICYHOLDER, Column14 as MAILING_COUNTY_POLICYHOLDER,
     Column15 as MAILING_LOT_UNIT_POLICYHOLDER, Column16 as MAILING_STATE_POLICYHOLDER, Column17 as MAILING_STREET_ADDRESS_POLICYHOLDER,
     Column18 as MAILING_ZIP_CODE_POLICYHOLDER, Column19 as MIDDLE_NAME, Column20 as PRIMARY_CONTACT_NUMBER,
      Column21 as THIRD_PARTY_NOTIFICATION_NON_CONSUMER, Column22 as TYPE_OF_INSURED, Column23 as ORGANIZATION_TYPE, Column24 as ORGANIZATION_NAME,
      Column25 as POLICYHOLDER_ID, Column26 as RELATIONSHIP_ORGANIZATION, Column27 as REP_FIRST_NAME, Column28 as REP_LAST_NAME
FROM VALUES
{% for entity in legacy_entities %}
    {% if entity %}
        {% set entity_json = fromjson(entity) %}
{# {{ log(entity_json['values'], info=True) }} #}
(
    '{{ legacy_pk[loop.index0] }}',
    '{{ entity_json.accountLocator or null }}',
    {{ entity_json.completed }},
    '{{ entity_json.createdTimestamp }}',
    '{{ tojson(entity_json.flags) }}',
    '{{ entity_json.locator }}',
    '{{ entity_json.revision }}',
    '{{ entity_json.updatedTimestamp }}',
    {% if 'values' in entity_json and 'emailAddress' in entity_json['values'] %}
        {% if entity_json['values']['emailAddress'] is iterable %}
    '{{ entity_json['values']['emailAddress'][0] | replace("'", "\\'") }}',
        {% else %}
    '{{ entity_json['values']['emailAddress'] | replace("'", "\\'") }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'firstName' in entity_json['values'] %}
        {% if entity_json['values']['firstName'] is iterable %}
    '{{ entity_json['values']['firstName'][0] }}',
        {% else %}
    '{{ entity_json['values']['firstName'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'lastName' in entity_json['values'] %}
        {% if entity_json['values']['lastName'] is iterable %}
    '{{ entity_json['values']['lastName'][0] | replace("'", "\\'") }}',
        {% else %}
    '{{ entity_json['values']['lastName'] | replace("'", "\\'") }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailingCityPolicyholder' in entity_json['values'] %}
        {% if entity_json['values']['mailingCityPolicyholder'] is iterable %}
    '{{ entity_json['values']['mailingCityPolicyholder'][0] }}',
        {% else %}
    '{{ entity_json['values']['mailingCityPolicyholder'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailingCountryPolicyholder' in entity_json['values'] %}
        {% if entity_json['values']['mailingCountryPolicyholder'] is iterable %}
    '{{ entity_json['values']['mailingCountryPolicyholder'][0] }}',
        {% else %}
    '{{ entity_json['values']['mailingCountryPolicyholder'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailingCountyPolicyholder' in entity_json['values'] %}
        {% if entity_json['values']['mailingCountyPolicyholder'] is iterable %}
    '{{ entity_json['values']['mailingCountyPolicyholder'][0] }}',
        {% else %}
    '{{ entity_json['values']['mailingCountyPolicyholder'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailingLotUnitPolicyholder' in entity_json['values'] %}
        {% if entity_json['values']['mailingLotUnitPolicyholder'] is iterable %}
    '{{ entity_json['values']['mailingLotUnitPolicyholder'][0] }}',
        {% else %}
    '{{ entity_json['values']['mailingLotUnitPolicyholder'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailingStatePolicyholder' in entity_json['values'] %}
        {% if entity_json['values']['mailingStatePolicyholder'] is iterable %}
    '{{ entity_json['values']['mailingStatePolicyholder'][0] }}',
        {% else %}
    '{{ entity_json['values']['mailingStatePolicyholder'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailingStreetAddressPolicyholder' in entity_json['values'] %}
        {% if entity_json['values']['mailingStreetAddressPolicyholder'] is iterable %}
    '{{ entity_json['values']['mailingStreetAddressPolicyholder'][0] | replace("'", "\\'") }}',
        {% else %}
    '{{ entity_json['values']['mailingStreetAddressPolicyholder'] | replace("'", "\\'") }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'mailingZipCodePolicyholder' in entity_json['values'] %}
        {% if entity_json['values']['mailingZipCodePolicyholder'] is iterable %}
    '{{ entity_json['values']['mailingZipCodePolicyholder'][0] }}',
        {% else %}
    '{{ entity_json['values']['mailingZipCodePolicyholder'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'middleName' in entity_json['values'] %}
        {% if entity_json['values']['middleName'] is iterable %}
    '{{ entity_json['values']['middleName'][0] }}',
        {% else %}
    '{{ entity_json['values']['middleName'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'primaryContactNumber' in entity_json['values'] %}
        {% if entity_json['values']['primaryContactNumber'] is iterable %}
    '{{ entity_json['values']['primaryContactNumber'][0] }}',
        {% else %}
    '{{ entity_json['values']['primaryContactNumber'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'thirdPartyNotificationNonConsumer' in entity_json['values'] %}
        {% if entity_json['values']['thirdPartyNotificationNonConsumer'] is iterable %}
    '{{ entity_json['values']['thirdPartyNotificationNonConsumer'][0] }}',
        {% else %}
    '{{ entity_json['values']['thirdPartyNotificationNonConsumer'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'typeOfInsured' in entity_json['values'] %}
        {% if entity_json['values']['typeOfInsured'] is iterable %}
    '{{ entity_json['values']['typeOfInsured'][0] }}',
        {% else %}
    '{{ entity_json['values']['typeOfInsured'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'organizationType' in entity_json['values'] %}
        {% if entity_json['values']['organizationType'] is iterable %}
    '{{ entity_json['values']['organizationType'][0] }}',
        {% else %}
    '{{ entity_json['values']['organizationType'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'organizationName' in entity_json['values'] %}
        {% if entity_json['values']['organizationName'] is iterable %}
    '{{ entity_json['values']['organizationName'][0] }}',
        {% else %}
    '{{ entity_json['values']['organizationName'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'policyholderId' in entity_json['values'] %}
        {% if entity_json['values']['policyholderId'] is iterable %}
    '{{ entity_json['values']['policyholderId'][0] }}',
        {% else %}
    '{{ entity_json['values']['policyholderId'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'relationshipOrganization' in entity_json['values'] %}
        {% if entity_json['values']['relationshipOrganization'] is iterable %}
    '{{ entity_json['values']['relationshipOrganization'][0] }}',
        {% else %}
    '{{ entity_json['values']['relationshipOrganization'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'repFirstName' in entity_json['values'] %}
        {% if entity_json['values']['repFirstName'] is iterable %}
    '{{ entity_json['values']['repFirstName'][0] }}',
        {% else %}
    '{{ entity_json['values']['repFirstName'] }}',
        {% endif %}
    {% else %}null,{% endif %}
    {% if 'values' in entity_json and 'repLastName' in entity_json['values'] %}
        {% if entity_json['values']['repLastName'] is iterable %}
    '{{ entity_json['values']['repLastName'][0] }}'
        {% else %}
    '{{ entity_json['values']['repLastName'] }}'
        {% endif %}
    {% else %}null{% endif %}
){% if not loop.last %},{% endif %}
    {% endif %}
{% endfor %}
{% endmacro %}