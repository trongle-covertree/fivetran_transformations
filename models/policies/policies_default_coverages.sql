
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

select pk, sk, pricing from dynamodb_develop.deve_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'DEFAULT_COVERAGES') and _fivetran_deleted='FALSE'
