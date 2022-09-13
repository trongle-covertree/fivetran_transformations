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

{% set char_field_group_keys = { quote_inception_date: None, auto_policy_with_agency: None, date_of_birth: None, reason_description: None, reason_code: None, prior_carrier_name: None, prior_policy_expiration_date: None, prior_insurance: None, additionalinsured_date_of_birth: None, ad_last_name: None, ad_first_name: None, relationship_to_policyholder: None, claim_amount: None, claim_number: None, description_of_loss: None, claim_cat: None, claim_source: None, category: None, claim_date: None, country: None, agency_phone_number: None, email_address: None, city: None, agent_id: None, lot_unit: None, agency_id: None, agency_contact_name: None, state: None, agency_license: None, street_address: None, zip_code: None, animal_bite: None, conviction: None, cancellation_renew: None, previous_street_address_policyholder: None, previous_country_policyholder: None, previous_zip_code_policyholder: None, previous_city_policyholder: None, previous_lot_unit_policyholder: None, previous_state_policyholder: None } %}
{% set char_field_value_keys = { association_discount: None, paperless_discount: None, multi_policy_discount: None, application_intiation: None, insurance_score: None } %}


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

    {% if 'quote_inception_date' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'quote_inception_date': char_json.fieldGroupsByLocator[field_group_key].quote_inception_date[0] }) %}
        {% if char_field_group_keys['quote_inception_date'] is not none %}'{{ char_field_group_keys['quote_inception_date']}}'{% else %}null{% endif %},
    {% endif %}
    {% if 'auto_policy_with_agency' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'auto_policy_with_agency': char_json.fieldGroupsByLocator[field_group_key].auto_policy_with_agency[0] }) %}
        {% if char_field_group_keys['auto_policy_with_agency'] is not none %}'{{ char_field_group_keys['auto_policy_with_agency']}}'{% else %}null{% endif %},
    {% endif %}
    {% if 'date_of_birth' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'date_of_birth': char_json.fieldGroupsByLocator[field_group_key].date_of_birth[0] }) %}
        {% if char_field_group_keys['date_of_birth'] is not none %}'{{ char_field_group_keys['date_of_birth']}}'{% else %}null{% endif %},
    {% endif %}
    {% if 'reason_description' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'reason_description': char_json.fieldGroupsByLocator[field_group_key].reason_description[0] }) %}
        {% if char_field_group_keys['reason_description'] is not none %}'{{ char_field_group_keys['reason_description']}}'{% else %}null{% endif %},
    {% endif %}
    {% if 'reason_code' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'reason_code': char_json.fieldGroupsByLocator[field_group_key].reason_code[0] }) %}
        {% if char_field_group_keys['reason_code'] is not none %}'{{ char_field_group_keys['reason_code']}}'{% else %}null{% endif %},
    {% endif %}
    {% if 'prior_carrier_name' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'prior_carrier_name': char_json.fieldGroupsByLocator[field_group_key].prior_carrier_name[0] }) %}
        {% if char_field_group_keys['prior_carrier_name'] is not none %}'{{ char_field_group_keys['prior_carrier_name']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'prior_policy_expiration_date' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'prior_policy_expiration_date': char_json.fieldGroupsByLocator[field_group_key].prior_policy_expiration_date[0] }) %}
        {% if char_field_group_keys['prior_policy_expiration_date'] is not none %}'{{ char_field_group_keys['prior_policy_expiration_date']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'prior_insurance' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'prior_insurance': char_json.fieldGroupsByLocator[field_group_key].prior_insurance[0] }) %}
        {% if char_field_group_keys['prior_insurance'] is not none %}'{{ char_field_group_keys['prior_insurance'] | replace("'", "\\'")}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'additionalinsured_date_of_birth' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'additionalinsured_date_of_birth': char_json.fieldGroupsByLocator[field_group_key].additionalinsured_date_of_birth[0] }) %}
        {% if char_field_group_keys['additionalinsured_date_of_birth'] is not none %}'{{ char_field_group_keys['additionalinsured_date_of_birth']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'ad_last_name' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'ad_last_name': char_json.fieldGroupsByLocator[field_group_key].ad_last_name[0] }) %}
        {% if char_field_group_keys['ad_last_name'] is not none %}'{{ char_field_group_keys['ad_last_name']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'ad_first_name' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'ad_first_name': char_json.fieldGroupsByLocator[field_group_key].ad_first_name[0] }) %}
        {% if char_field_group_keys['ad_first_name'] is not none %}'{{ char_field_group_keys['ad_first_name']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'relationship_to_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'relationship_to_policyholder': char_json.fieldGroupsByLocator[field_group_key].relationship_to_policyholder[0] }) %}
        {% if char_field_group_keys['relationship_to_policyholder'] is not none %}'{{ char_field_group_keys['relationship_to_policyholder']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'claim_amount' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'claim_amount': char_json.fieldGroupsByLocator[field_group_key].claim_amount[0] }) %}
        {% if char_field_group_keys['claim_amount'] is not none %}'{{ char_field_group_keys['claim_amount']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'claim_number' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'claim_number': char_json.fieldGroupsByLocator[field_group_key].claim_number[0] }) %}
        {% if char_field_group_keys['claim_number'] is not none %}'{{ char_field_group_keys['claim_number']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'description_of_loss' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'description_of_loss': char_json.fieldGroupsByLocator[field_group_key].description_of_loss[0] }) %}
        {% if char_field_group_keys['description_of_loss'] is not none %}'{{ char_field_group_keys['description_of_loss']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'claim_cat' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'claim_cat': char_json.fieldGroupsByLocator[field_group_key].claim_cat[0] }) %}
        {% if char_field_group_keys['claim_cat'] is not none %}'{{ char_field_group_keys['claim_cat']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'claim_source' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'claim_source': char_json.fieldGroupsByLocator[field_group_key].claim_source[0] }) %}
        {% if char_field_group_keys['claim_source'] is not none %}'{{ char_field_group_keys['claim_source']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'category' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'category': char_json.fieldGroupsByLocator[field_group_key].category[0] }) %}
        {% if char_field_group_keys['category'] is not none %}'{{ char_field_group_keys['category']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'claim_date' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'claim_date': char_json.fieldGroupsByLocator[field_group_key].claim_date[0] }) %}
        {% if char_field_group_keys['quote_inception_date'] is not none %}'{{ char_field_group_keys['quote_inception_date']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'country' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'country': char_json.fieldGroupsByLocator[field_group_key].country[0] }) %}
        {% if char_field_group_keys['country'] is not none %}'{{ char_field_group_keys['country']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'agency_phone_number' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'agency_phone_number': char_json.fieldGroupsByLocator[field_group_key].agency_phone_number[0] }) %}
        {% if char_field_group_keys['agency_phone_number'] is not none %}'{{ char_field_group_keys['agency_phone_number']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'email_address' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'email_address': char_json.fieldGroupsByLocator[field_group_key].email_address[0] }) %}
        {% if char_field_group_keys['email_address'] is not none %}'{{ char_field_group_keys['email_address']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'city' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'city': char_json.fieldGroupsByLocator[field_group_key].city[0] }) %}
        {% if char_field_group_keys['city'] is not none %}'{{ char_field_group_keys['city']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'agent_id' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'agent_id': char_json.fieldGroupsByLocator[field_group_key].agent_id[0] }) %}
        {% if char_field_group_keys['agent_id'] is not none %}'{{ char_field_group_keys['agent_id']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'lot_unit' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'lot_unit': char_json.fieldGroupsByLocator[field_group_key].lot_unit[0] }) %}
        {% if char_field_group_keys['lot_unit'] is not none %}'{{ char_field_group_keys['lot_unit']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'agency_id' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'agency_id': char_json.fieldGroupsByLocator[field_group_key].agency_id[0] }) %}
        {% if char_field_group_keys['quote_inception_date'] is not none %}'{{ char_field_group_keys['quote_inception_date']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'agency_contact_name' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'agency_contact_name': char_json.fieldGroupsByLocator[field_group_key].agency_contact_name[0] }) %}
        {% if char_field_group_keys['agency_contact_name'] is not none %}'{{ char_field_group_keys['agency_contact_name']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'state' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'state': char_json.fieldGroupsByLocator[field_group_key].state[0] }) %}
        {% if char_field_group_keys['state'] is not none %}'{{ char_field_group_keys['state']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'agency_license' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'agency_license': char_json.fieldGroupsByLocator[field_group_key].agency_license[0] }) %}
        {% if char_field_group_keys['agency_license'] is not none %}'{{ char_field_group_keys['agency_license']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'street_address' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'street_address': char_json.fieldGroupsByLocator[field_group_key].street_address[0] }) %}
        {% if char_field_group_keys['street_address'] is not none %}'{{ char_field_group_keys['street_address']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'zip_code' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'zip_code': char_json.fieldGroupsByLocator[field_group_key].zip_code[0] }) %}
        {% if char_field_group_keys['zip_code'] is not none %}'{{ char_field_group_keys['zip_code']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'animal_bite' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'animal_bite': char_json.fieldGroupsByLocator[field_group_key].animal_bite[0] }) %}
        {% if char_field_group_keys['animal_bite'] is not none %}'{{ char_field_group_keys['animal_bite']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'conviction' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'conviction': char_json.fieldGroupsByLocator[field_group_key].conviction[0] }) %}
        {% if char_field_group_keys['conviction'] is not none %}'{{ char_field_group_keys['conviction']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'cancellation_renew' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'cancellation_renew': char_json.fieldGroupsByLocator[field_group_key].cancellation_renew[0] }) %}
        {% if char_field_group_keys['cancellation_renew'] is not none %}'{{ char_field_group_keys['cancellation_renew']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'previous_street_address_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'previous_street_address_policyholder': char_json.fieldGroupsByLocator[field_group_key].previous_street_address_policyholder[0] }) %}
        {% if char_field_group_keys['previous_street_address_policyholder'] is not none %}'{{ char_field_group_keys['previous_street_address_policyholder']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'previous_country_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'previous_country_policyholder': char_json.fieldGroupsByLocator[field_group_key].previous_country_policyholder[0] }) %}
        {% if char_field_group_keys['previous_country_policyholder'] is not none %}'{{ char_field_group_keys['previous_country_policyholder']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'previous_zip_code_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'previous_zip_code_policyholder': char_json.fieldGroupsByLocator[field_group_key].previous_zip_code_policyholder[0] }) %}
        {% if char_field_group_keys['previous_zip_code_policyholder'] is not none %}'{{ char_field_group_keys['previous_zip_code_policyholder']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'previous_city_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'previous_city_policyholder': char_json.fieldGroupsByLocator[field_group_key].previous_city_policyholder[0] }) %}
        {% if char_field_group_keys['previous_city_policyholder'] is not none %}'{{ char_field_group_keys['previous_city_policyholder']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'previous_lot_unit_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'previous_lot_unit_policyholder': char_json.fieldGroupsByLocator[field_group_key].previous_lot_unit_policyholder[0] }) %}
        {% if char_field_group_keys['previous_lot_unit_policyholder'] is not none %}'{{ char_field_group_keys['previous_lot_unit_policyholder']}}'{% else %}null{% endif %},
     {% endif %}
    {% if 'previous_state_policyholder' in char_json['fieldGroupsByLocator'][field_group_key] %}
        {% do char_field_group_keys.update({ 'previous_state_policyholder': char_json.fieldGroupsByLocator[field_group_key].previous_state_policyholder[0] }) %}
        {% if char_field_group_keys['previous_state_policyholder'] is not none %}'{{ char_field_group_keys['previous_state_policyholder']}}'{% else %}null{% endif %},
     {% endif %}
            {% endfor %}
{# fieldValues #}
    {% if 'association_discount' in char_json['fieldValues'] %}
        {% do char_field_value_keys.update({ 'association_discount': char_json.fieldValues.association_discount[0] }) %}
        {% if char_field_value_keys['association_discount'] is not none %}'{{ char_field_value_keys['association_discount']}}'{% else %}null{% endif %},
    {% endif %}
    {% if 'paperless_discount' in char_json['fieldValues'] %}
        {% do char_field_value_keys.update({ 'paperless_discount': char_json.fieldValues.paperless_discount[0] }) %}
        {% if char_field_value_keys['paperless_discount'] is not none %}'{{ char_field_value_keys['paperless_discount']}}'{% else %}null{% endif %},
    {% endif %}
    {% if 'multi_policy_discount' in char_json['fieldValues'] %}
        {% do char_field_value_keys.update({ 'multi_policy_discount': char_json.fieldValues.multi_policy_discount[0] }) %}
        {% if char_field_value_keys['multi_policy_discount'] is not none %}'{{ char_field_value_keys['multi_policy_discount']}}'{% else %}null{% endif %},
    {% endif %}
    {% if 'application_intiation' in char_json['fieldValues'] %}
        {% do char_field_value_keys.update({ 'application_intiation': char_json.fieldValues.application_intiation[0] }) %}
        {% if char_field_value_keys['application_intiation'] is not none %}'{{ char_field_value_keys['application_intiation']}}'{% else %}null{% endif %},
    {% endif %}
    {% if 'insurance_score' in char_json['fieldValues'] %}
        {% do char_field_value_keys.update({ 'insurance_score': char_json.fieldValues.insurance_score[0] }) %}
        {% if char_field_value_keys['insurance_score'] is not none %}'{{ char_field_value_keys['insurance_score']}}'{% else %}null{% endif %},
    {% endif %}
    {# {% if 'fieldValues' in char_json and 'association_discount' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['association_discount'][0] }}'{% else %}null{% endif %},
    {% if 'fieldValues' in char_json and 'paperless_discount' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['paperless_discount'][0] }}'{% else %}null{% endif %},
    {% if 'fieldValues' in char_json and 'multi_policy_discount' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['multi_policy_discount'][0] }}'{% else %}null{% endif %},
    {% if 'fieldValues' in char_json and 'application_intiation' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['application_intiation'][0] }}'{% else %}null{% endif %},
    {% if 'fieldValues' in char_json and 'insurance_score' in char_json['fieldValues'] %}'{{ char_json['fieldValues']['insurance_score'][0] }}'{% else %}null{% endif %}, #}
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