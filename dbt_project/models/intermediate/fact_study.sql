SELECT
    -- Identification
    nct_id,
    brief_title,
    official_title,
    acronym,
    org_study_id,
    org_full_name,
    org_class,

    -- Sponsor
    lead_sponsor_name,
    lead_sponsor_class,

    -- Status
    overall_status,
    last_known_status,
    why_stopped,
    status_verified_date,
    has_expanded_access,

    -- Dates
    start_date,
    start_date_type,
    primary_completion_date,
    primary_completion_date_type,
    completion_date,
    completion_date_type,
    study_first_submit_date,
    study_first_submit_qc_date,
    study_first_post_date,
    last_update_submit_date,
    last_update_post_date,
    results_first_post_date,

    has_dmc,
    is_fda_regulated_drug,
    is_fda_regulated_device,

    -- Design
    study_type,
    phases,
    enrollment_count,
    enrollment_type,
    design_allocation,
    intervention_model,
    primary_purpose,
    observational_model,
    masking,
    target_duration,

    eligibility_sex,
    gender_based,
    minimum_age,
    maximum_age,
    healthy_volunteers,

    ipd_sharing,
    has_results,

    -- The question "Timeline analysis of study durations" is a bit vague, and the data
    -- had a ton of date fields. In the end, I decided to go with the simple and obvious solution.
    datediff('day', start_date, completion_date)	AS duration_days

FROM {{ ref('stg_study') }}
