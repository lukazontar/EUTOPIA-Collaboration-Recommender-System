with
    api_orcid_employment_json as (
        select
            orcid_id as orcid_id,
            university_name,
            json,
            ifnull(
                affiliation,
                json_extract(json, '$.activities-summary.employments.affiliation-group')
            ) as affiliation_json,
            affiliation_summary as affiliation_summary_json
        from {{ source("ORCID", "ORCID_MEMBER") }}
        left join
            unnest(
                json_extract_array(
                    json_extract(
                        json, '$.activities-summary.employments.affiliation-group'
                    )
                )
            ) affiliation
        left join
            unnest(
                json_extract_array(json_extract(affiliation, '$.summaries'))
            ) affiliation_summary
    ),
    api_orcid_summary_by_employment as (
        select
            orcid_id,
            university_name,
            json_extract_scalar(
                affiliation_summary_json, '$.employment-summary.department-name'
            ) as department_name,
            json_extract_scalar(
                affiliation_summary_json, '$.employment-summary.role-title'
            ) as role_title,
            json_extract(
                affiliation_summary_json, '$.employment-summary.start-date.year.value'
            ) as start_year,
            json_extract(
                affiliation_summary_json, '$.employment-summary.start-date.month.value'
            ) as start_month,
            json_extract(
                affiliation_summary_json, '$.employment-summary.start-date.day.value'
            ) as start_day,
            json_extract(
                affiliation_summary_json, '$.employment-summary.end-date.year.value'
            ) as end_year,
            json_extract(
                affiliation_summary_json, '$.employment-summary.end-date.month.value'
            ) as end_month,
            json_extract(
                affiliation_summary_json, '$.employment-summary.end-date.day.value'
            ) as end_day,
            json_extract_scalar(
                affiliation_summary_json, '$.employment-summary.organization.name'
            ) as organization_name,
            json_extract_scalar(
                affiliation_summary_json,
                '$.employment-summary.organization.address.city'
            ) as organization_city,
            json_extract_scalar(
                affiliation_summary_json,
                '$.employment-summary.organization.address.region'
            ) as organization_region,
            json_extract_scalar(
                affiliation_summary_json,
                '$.employment-summary.organization.address.country'
            ) as organization_country,
            json_extract_scalar(
                affiliation_summary_json, '$.employment-summary.url'
            ) as organization_url
        from api_orcid_employment_json
    )
select *
from api_orcid_summary_by_employment
