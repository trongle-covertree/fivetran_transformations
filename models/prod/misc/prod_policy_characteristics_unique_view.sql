{{ config(materialized='table') }}

select *
FROM
(
    SELECT c.pk, quote_inception_date, auto_policy_with_agency, date_of_birth, reason_description, reason_code, prior_carrier_name, prior_policy_expiration_date,
        prior_insurance, additionalinsured_date_of_birth, ad_last_name, ad_first_name, relationship_to_policyholder, claim_amount, claim_number, description_of_loss,
        claim_cat, claim_source, category, claim_date, country, agency_phone_number, email_address, city, trim(regexp_replace(agent_id, '[^a-zA-Z0-9 ]+')) as agent_id, lot_unit,
        trim(regexp_replace(agency_id, '[^a-zA-Z0-9 ]+')) as agency_id, upper(trim(regexp_replace(agency_contact_name, '[^a-zA-Z0-9 ]+'))) as agency_contact_name, state, agency_license, street_address, zip_code, animal_bite, conviction, cancellation_renew,
        previous_street_address_policyholder, previous_country_policyholder, previous_zip_code_policyholder, previous_city_policyholder, previous_lot_unit_policyholder,
        previous_state_policyholder, association_discount, paperless_discount, multi_policy_discount, APPLICATION_INTIATION, insurance_score, gross_premium,
        gross_premium_currency, gross_taxes, gross_taxes_currency, c.created_timestamp, c.updated_timestamp, end_timestamp, c.issued_timestamp, c.locator, media_by_locator,
        policy_start_timestamp, policy_end_timestamp, policy_locator, c.policyholder_locator, c.product_locator, start_timestamp, tax_groups, policy_created_timestamp,
        policy_updated_timestamp, upper(trim(agent_on_record)) as agent_on_record, replaced_timestamp,
        ROW_NUMBER() OVER (PARTITION BY c.pk ORDER BY c.created_timestamp desc, start_timestamp desc) num
    FROM fivetran_covertree.transformations_dynamodb.prod_policy_characteristics as c join fivetran_covertree.transformations_dynamodb.prod_policies_policy as p on p.pk = c.pk
    where (start_timestamp < current_timestamp() and end_timestamp > current_timestamp() and replaced_timestamp is null)
        or (start_timestamp = end_timestamp) or (current_timestamp() < start_timestamp and replaced_timestamp is null)
        or (p.pk not like 'POLICY#%' and p.status = 'Policy-Activated')
) inn
where inn.num = 1