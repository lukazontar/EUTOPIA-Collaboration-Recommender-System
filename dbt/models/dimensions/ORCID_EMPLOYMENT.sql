with
    merged_orcid_summary_by_employment as (
        select *
        from {{ ref("STG_API_ORCID_EMPLOYMENT") }}
        union all
        select *
        from {{ ref("STG_ONETIME_ORCID_EMPLOYMENT") }}
    )
select distinct
    orcid_id,
    university_name,
    department_name,
    role_title,
    date(
        cast(start_year as int64),
        ifnull(cast(start_month as int64), 1),
        ifnull(cast(start_day as int64), 1)
    ) as start_dt,
    date(
        cast(end_year as int64),
        ifnull(cast(end_month as int64), 1),
        ifnull(cast(end_day as int64), 1)
    ) as end_dt,
    organization_name,
    organization_city,
    organization_region,
    organization_country,
    organization_url
from merged_orcid_summary_by_employment
