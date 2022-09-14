# Covertree Fivetran Transformations
This repo contains the transformations that pull data from DynamoDB, Google Ads/Analytics, and Hubspot and loads them into Snowflake

## Local Development Setup
- Before starting, please set up your SSH key. Instructions can be found [here](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
- Clone this repo
- Install DBT through the dbt website [here](https://docs.getdbt.com/dbt-cli/install/homebrew)
- After installing DBT locally you'll have to setup your profiles.yaml that's in `~/.dbt` to the below:
```
fivetran_transformations:
  outputs:
    dev:
      account: {ACCOUNT_NAME}
      database: FIVETRAN_COVERTREE
      password: {YOUR_PASSWORD}
      role: ACCOUNTADMIN
      schema: TRANSFORMATIONS_DYNAMODB_DEVELOP
      threads: 1
      type: snowflake
      user: {YOUR_USERNAME}
      warehouse: FIVETRAN_WAREHOUSE
  target: dev
```
- Go into your terminal, navigate to the project, and run the command `dbt compile`
- Navigate to the directory `fivetran_transformations/target/compiled/fivetran_transformations` and see if some files are created that have generated queries based off the models and macros in this project. If so then everything's connected and running properly