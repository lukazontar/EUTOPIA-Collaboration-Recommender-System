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
    if(
        start_year is null,
        date(start_year, ifnull(start_month, 1), ifnull(start_day, 1))
    ) as start_dt,
    if(
        end_year is null, date(end_year, ifnull(end_month, 1), ifnull(end_day, 1))
    ) as end_dt,
    organization_name,
    organization_city,
    organization_region,
    organization_country,
    organization_url
from merged_orcid_summary_by_employment
