# Credentials

Most, but not all, credentials should be stored in file: `secrets/secrets.yml`.

## Google Cloud Platform

Secrets required for Google Cloud Platform are stored in `secrets/service_account_key.json`.

To see how to create a service account key, see
here: [Google Cloud Platform: Create and delete service account keys](https://cloud.google.com/iam/docs/keys-create-delete).

## dbt Cloud

Secrets required for dbt Cloud are stored in `~/.dbt/dbt_cloud.yml`.

To see how to configure the dbt Cloud CLI, see
here: [dbt: Configure and use the dbt Cloud CLI](https://docs.getdbt.com/docs/cloud/configure-cloud-cli).

## Mage AI

To see how to set up Mage AI secrets in the UI, see
here: [Mage AI: Secrets](https://docs.mage.ai/development/variables/secrets).

For each, pipeline, secrets are defined in more detailed documentations in `docs/mage` directory.

## ORCID API

Secrets required for ORCID API are:

- `ORCID_CLIENT_ID`: ORCID Client ID.
- `ORCID_CLIENT_SECRET`: ORCID Client Secret.

To see how to register a public API client, see
here: [ORCID: Registering a Public API client](https://info.orcid.org/documentation/integration-guide/registering-a-public-api-client/).