{{ config(materialized='table') }}

select pk, payments[0]:amount::double as amount, payments[0]:amountCurrency::varchar(3) as amount_currency, payments[0]:displayId::varchar(256) as display_id,
    payments[0]:fieldValues:stripe_id[0]::varchar as stripe_id, payments[0]:invoiceLocator::varchar as invoice_locator, payments[0]:locator::varchar as locator,
    payments[0]:mediaByLocator as media_by_locator, payments[0]:postedTimestamp::timestamp as posted_timestamp
from fivetran_covertree.transformations_dynamodb.prod_policies_cancellation
where array_size(payments) > 0 and payments is not null and array_size(payments) = 1
