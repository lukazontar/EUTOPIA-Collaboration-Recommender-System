with
    onetime_orcid_employment_json as (
        select
            json_extract_scalar(
                json, '$.record:record.common:orcid-identifier.common:path'
            ) as orcid_id,
            university_name,
            json,
            ifnull(
                affiliation,
                json_extract(
                    json,
                    '$.record:record.activities:activities-summary.activities:employments.activities:affiliation-group'
                )
            ) as affiliation_json,
            ifnull(
                json_extract(affiliation, '$.employment:employment-summary'),
                json_extract(
                    json,
                    '$.record:record.activities:activities-summary.activities:employments.activities:affiliation-group.employment:employment-summary'
                )
            ) as affiliation_summary_json
        from {{ source("ORCID", "ONETIME_ORCID_MEMBER") }}
        left join
            unnest(
                json_extract_array(
                    json_extract(
                        json,
                        '$.record:record.activities:activities-summary.activities:employments.activities:affiliation-group'
                    )
                )
            ) affiliation
    ),
    onetime_summary_by_employment as (
        select
            json_extract_scalar(
                json, '$.record:record.common:orcid-identifier.common:path'
            ) as orcid_id,
            university_name,
            json_extract_scalar(
                affiliation_summary_json, '$.common:department-name'
            ) as department_name,
            json_extract_scalar(
                affiliation_summary_json, '$.common:role-title'
            ) as role_title,
            json_extract_scalar(
                affiliation_summary_json, '$.common:start-date.common:year'
            ) as start_year,
            json_extract_scalar(
                affiliation_summary_json, '$.common:start-date.common:month'
            ) as start_month,
            json_extract_scalar(
                affiliation_summary_json, '$.common:start-date.common:day'
            ) as start_day,
            json_extract_scalar(
                affiliation_summary_json, '$.common:end-date.common:year'
            ) as end_year,
            json_extract_scalar(
                affiliation_summary_json, '$.common:end-date.common:month'
            ) as end_month,
            json_extract_scalar(
                affiliation_summary_json, '$.common:end-date.common:day'
            ) as end_day,
            json_extract_scalar(
                affiliation_summary_json, '$.common:organization.common:name'
            ) as organization_name,
            json_extract_scalar(
                affiliation_summary_json,
                '$.common:organization.common:address.common:city'
            ) as organization_city,
            json_extract_scalar(
                affiliation_summary_json,
                '$.common:organization.common:address.common:region'
            ) as organization_region,
            json_extract_scalar(
                affiliation_summary_json,
                '$.common:organization.common:address.common:country'
            ) as organization_country,
            json_extract_scalar(
                affiliation_summary_json, '$.common:url'
            ) as organization_url
        from onetime_orcid_employment_json
    )
select *
from onetime_summary_by_employment
