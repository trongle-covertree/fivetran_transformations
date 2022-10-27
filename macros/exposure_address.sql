{% macro run_exposures_address(env, prefix) %}

{% set exposures_address_query %}
select exposures, pk, created_timestamp, updated_timestamp
from {{ env }}.{{ prefix }}_policies_policy
{% if is_incremental() %}
  WHERE created_timestamp > (select policy_created_timestamp from {{ env }}.{{ prefix }}_policy_exposures_address order by created_timestamp desc limit 1)
      or updated_timestamp > (select policy_updated_timestamp from {{ env }}.{{ prefix }}_policy_exposures_address order by updated_timestamp desc limit 1)
{% endif %}
{% endset %}

{% set results = run_query(exposures_address_query) %}

{% if execute %}
    {% set exposures = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
    {% set created_timestamps = results.columns[2].values() %}
    {% set updated_timestamps = results.columns[3].values() %}
{% endif %}

{% if exposures|length > 0 %}
SELECT Column1 AS ID, Column2 AS PK, Column3 AS STREET_ADDRESS, Column4 AS LOT_UNIT, Column5 AS CITY, Column6 AS STATE, Column7 AS ZIP_CODE,
    Column8 AS COUNTY, Column9 AS COUNTRY, to_timestamp(Column10) AS CREATED_TIMESTAMP, to_timestamp(Column11) AS UPDATED_TIMESTAMP,
    to_timestamp(Column12) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column13) AS POLICY_UPDATED_TIMESTAMP FROM VALUES
    {% for exposure in exposures %}
        {% set outer_loop = loop %}
        {% if exposure %}
            {% set exposure_arr = fromjson(exposure) %}
            {% for exposure_json in exposure_arr %}
                {% set exposure_json_loop = loop %}
                {% for char in exposure_json.characteristics if exposure_json.name != 'Policy Level Coverages' %}
                    {% set address_char_keys = { id: none, country: none, city: none, lot_unit: none, state: none, county: none, street_address: none, zip_code: none, created_timestamp: none, updated_timestamp: none } %}
                    {% for current_char_key in char.fieldGroupsByLocator.keys() %}
                        {% if 'street_address' in char.fieldGroupsByLocator[current_char_key]
                            and ('officer_first_name' not in char.fieldGroupsByLocator[current_char_key] or 'officer_last_name' not in char.fieldGroupsByLocator[current_char_key] or 'officer_mail_address' not in char.fieldGroupsByLocator[current_char_key])
                            and ('account_number' not in char.fieldGroupsByLocator[current_char_key] or 'additional_interest_id' not in char.fieldGroupsByLocator[current_char_key]) %}

                            {% do address_char_keys.update({ 'country': char.fieldGroupsByLocator[current_char_key].country[0] }) %}
                            {% do address_char_keys.update({ 'city': char.fieldGroupsByLocator[current_char_key].city[0] }) %}
                            {% if 'lot_unit' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do address_char_keys.update({ 'lot_unit': char.fieldGroupsByLocator[current_char_key].lot_unit[0] }) %}
                            {% endif %}
                            {% do address_char_keys.update({ 'state': char.fieldGroupsByLocator[current_char_key].state[0] }) %}
                            {% if 'county' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do address_char_keys.update({ 'county': char.fieldGroupsByLocator[current_char_key].county[0] }) %}
                            {% endif %}
                            {% do address_char_keys.update({ 'street_address': char.fieldGroupsByLocator[current_char_key].street_address[0] }) %}
                            {% do address_char_keys.update({ 'zip_code': char.fieldGroupsByLocator[current_char_key].zip_code[0] }) %}
                            {% do address_char_keys.update({ 'id': char.locator }) %}
                            {% do address_char_keys.update({ 'created_timestamp': char.createdTimestamp }) %}
                            {% do address_char_keys.update({ 'updated_timestamp': char.updatedTimestamp }) %}
    (
        {% if address_char_keys.id|length > 0 %}'{{ address_char_keys.id }}'{% else %}null{% endif %},
        '{{ pk[outer_loop.index0] }}',
        {% if address_char_keys.street_address|length > 0 %}'{{ address_char_keys.street_address | replace("'", "\\'") }}'{% else %}null{% endif %},
        {% if address_char_keys.lot_unit|length > 0 %}'{{ address_char_keys.lot_unit }}'{% else %}null{% endif %},
        {% if address_char_keys.city|length > 0 %}'{{ address_char_keys.city | replace("'", "\\'") }}'{% else %}null{% endif %},
        {% if address_char_keys.state|length > 0 %}'{{ address_char_keys.state }}'{% else %}null{% endif %},
        {% if address_char_keys.zip_code|length > 0 %}'{{ address_char_keys.zip_code }}'{% else %}null{% endif %},
        {% if address_char_keys.county|length > 0 %}'{{ address_char_keys.county }}'{% else %}null{% endif %},
        {% if address_char_keys.country|length > 0 %}'{{ address_char_keys.country }}'{% else %}null{% endif %},
        {% if address_char_keys.created_timestamp|length > 0 %}'{{ address_char_keys.created_timestamp }}'{% else %}null{% endif %},
        {% if address_char_keys.updated_timestamp|length > 0 %}'{{ address_char_keys.updated_timestamp }}'{% else %}null{% endif %},
        '{{ created_timestamps[outer_loop.index0] }}',
        '{{ updated_timestamps[outer_loop.index0] }}'
    ){% if not outer_loop.last or (outer_loop.last and exposure_json_loop.index0 != exposure_arr|length - 2) %},{% endif %}
                        {% endif %}
                    {% endfor %}
                {% endfor %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endif %}
{% endmacro %}