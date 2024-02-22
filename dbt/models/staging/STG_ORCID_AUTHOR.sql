select
    orcid_id,
    university_name,
    json_extract_scalar(
        json, '$.person.name.given-names.value'
    ) as orcid_member_given_name,
    json_extract_scalar(
        json, '$.person.name.family-name.value'
    ) as orcid_member_family_name,
    json_extract_scalar(json, '$.orcid-identifier.uri') as orcid_member_url,
    json_extract_scalar(json, '$.orcid-identifier.path') as orcid_member_id,
    json_extract_scalar(json, '$.preferences.locale') as orcid_member_locale,
    cast(
        timestamp_millis(
            cast(
                json_extract_scalar(json, '$.history.last-modified-date.value') as int64
            )
        ) as date
    ) as orcid_member_last_modified_dt,
    json_extract_scalar(
        json, '$.history.creation-method'
    ) as orcid_member_creation_method,
    json_extract_scalar(json, '$.history.verified-email') as is_orcid_member_verified,
    string_agg(json_extract(keyword, '$.content'), ',') as keywords
from {{ source("ORCID", "ORCID_MEMBER") }}
left join
    unnest(json_extract_array(json_extract(json, '$.person.keywords.keyword'))) keyword
group by
    orcid_id,
    university_name,
    orcid_member_given_name,
    orcid_member_family_name,
    orcid_member_url,
    orcid_member_id,
    orcid_member_locale,
    orcid_member_last_modified_dt,
    orcid_member_creation_method,
    is_orcid_member_verified
union all
select
    json_extract_scalar(
        json, '$.record:record.common:orcid-identifier.common:path'
    ) as orcid_id,
    university_name as university_name,
    json_extract_scalar(
        json, '$.record:record.person:person.person:name.personal-details:given-names'
    ) as orcid_member_given_name,
    json_extract_scalar(
        json, '$.record:record.person:person.person:name.personal-details:family-name'
    ) as orcid_member_family_name,
    json_extract_scalar(
        json, '$.record:record.common:orcid-identifier.common:uri'
    ) as orcid_member_url,
    json_extract_scalar(
        json, '$.record:record.common:orcid-identifier.common:path'
    ) as orcid_member_id,
    json_extract_scalar(
        json, '$.record:record.preferences:preferences.preferences:locale'
    ) as orcid_member_locale,

    cast(
        cast(
            json_extract_scalar(
                json, '$.record:record.history:history.common:last-modified-date'
            ) as timestamp
        ) as date
    ) as orcid_member_last_modified_dt,
    json_extract_scalar(
        json, '$.record:record.history:history.history:creation-method'
    ) as orcid_member_creation_method,
    json_extract_scalar(
        json, '$.record:record.history:history.history:verified-email'
    ) as is_orcid_member_verified,
    string_agg(json_extract(keyword, '$.content'), ',') as keywords
from {{ source("ORCID", "ONETIME_ORCID_MEMBER") }}
left join
    unnest(
        json_extract_array(
            json_extract(json, '$.record:record.person:person.keyword:keywords')
        )
    ) keyword
group by
    orcid_id,
    university_name,
    orcid_member_given_name,
    orcid_member_family_name,
    orcid_member_url,
    orcid_member_id,
    orcid_member_locale,
    orcid_member_last_modified_dt,
    orcid_member_creation_method,
    is_orcid_member_verified
