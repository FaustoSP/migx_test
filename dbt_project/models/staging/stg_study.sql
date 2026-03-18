WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_studies') }}
),

-- The date fields in this dataset come in two formats: 'YYYY-MM-DD' and 'YYYY-MM'.
-- To solve this
-- The _type companion column (ACTUAL vs ESTIMATED) is kept alongside because it is analytically meaningful.
flattened AS (
    SELECT
        -- Identification
        protocolSection.identificationModule.nctId	AS nct_id,
        protocolSection.identificationModule.briefTitle	AS brief_title,
        protocolSection.identificationModule.officialTitle	AS official_title,
        protocolSection.identificationModule.acronym	AS acronym,
        protocolSection.identificationModule.orgStudyIdInfo.id	AS org_study_id,
        protocolSection.identificationModule.organization.fullName	AS org_full_name,
        protocolSection.identificationModule.organization.class	AS org_class,

        -- Sponsor
        protocolSection.sponsorCollaboratorsModule.leadSponsor.name	AS lead_sponsor_name,
        protocolSection.sponsorCollaboratorsModule.leadSponsor.class	AS lead_sponsor_class,

        -- Status
        protocolSection.statusModule.overallStatus	AS overall_status,
        protocolSection.statusModule.lastKnownStatus	AS last_known_status,
        protocolSection.statusModule.whyStopped	AS why_stopped,
        protocolSection.statusModule.statusVerifiedDate	AS status_verified_date,
        protocolSection.statusModule.expandedAccessInfo.hasExpandedAccess	AS has_expanded_access,

        -- Minor improvement: in a prod environment I would write a function instead of repeating this conditional several times
        TRY_STRPTIME(
            CASE
                WHEN length(protocolSection.statusModule.startDateStruct.date) = 7
                THEN protocolSection.statusModule.startDateStruct.date || '-01'
                ELSE protocolSection.statusModule.startDateStruct.date
            END,
            '%Y-%m-%d'
        )::DATE	AS start_date,
        protocolSection.statusModule.startDateStruct.type	AS start_date_type,

        TRY_STRPTIME(
            CASE
                WHEN length(protocolSection.statusModule.primaryCompletionDateStruct.date) = 7
                THEN protocolSection.statusModule.primaryCompletionDateStruct.date || '-01'
                ELSE protocolSection.statusModule.primaryCompletionDateStruct.date
            END,
            '%Y-%m-%d'
        )::DATE	AS primary_completion_date,
        protocolSection.statusModule.primaryCompletionDateStruct.type	AS primary_completion_date_type,

        TRY_STRPTIME(
            CASE
                WHEN length(protocolSection.statusModule.completionDateStruct.date) = 7
                THEN protocolSection.statusModule.completionDateStruct.date || '-01'
                ELSE protocolSection.statusModule.completionDateStruct.date
            END,
            '%Y-%m-%d'
        )::DATE	AS completion_date,
        protocolSection.statusModule.completionDateStruct.type	AS completion_date_type,

        -- According to the documentation this are always YYYY-MM-DD
        TRY_CAST(protocolSection.statusModule.studyFirstSubmitDate AS DATE)	AS study_first_submit_date,
        TRY_CAST(protocolSection.statusModule.studyFirstSubmitQcDate AS DATE)	AS study_first_submit_qc_date,
        TRY_CAST(
            protocolSection.statusModule.studyFirstPostDateStruct.date AS DATE
        )	AS study_first_post_date,
        TRY_CAST(protocolSection.statusModule.lastUpdateSubmitDate AS DATE)	AS last_update_submit_date,
        TRY_CAST(
            protocolSection.statusModule.lastUpdatePostDateStruct.date AS DATE
        )	AS last_update_post_date,
        TRY_CAST(
            protocolSection.statusModule.resultsFirstPostDateStruct.date AS DATE
        )	AS results_first_post_date,
        -- I had a weird, inconsistent error with this field. I think that its because this field is not always present,
        -- which means that duckdb doesn't always have this column.
        -- Possible fix have the schema predefined (and ideally, kept inside some kind of version control).
        -- However, that would have been overengineering for this test.
        --protocolSection.statusModule.resultsWaived	AS results_waived,

        -- Oversight flags
        protocolSection.oversightModule.oversightHasDmc	AS has_dmc,
        protocolSection.oversightModule.isFdaRegulatedDrug	AS is_fda_regulated_drug,
        protocolSection.oversightModule.isFdaRegulatedDevice	AS is_fda_regulated_device,

        -- Design
        protocolSection.designModule.studyType	AS study_type,
        -- Phases is a list of enum strings so it can be easily converted
        array_to_string(protocolSection.designModule.phases, '/')	AS phases,
        protocolSection.designModule.enrollmentInfo.count	AS enrollment_count,
        protocolSection.designModule.enrollmentInfo.type	AS enrollment_type,
        protocolSection.designModule.designInfo.allocation	AS design_allocation,
        protocolSection.designModule.designInfo.interventionModel	AS intervention_model,
        protocolSection.designModule.designInfo.primaryPurpose	AS primary_purpose,
        protocolSection.designModule.designInfo.observationalModel	AS observational_model,
        protocolSection.designModule.designInfo.maskingInfo.masking	AS masking,
        protocolSection.designModule.targetDuration	AS target_duration,

        protocolSection.eligibilityModule.sex	AS eligibility_sex,
        protocolSection.eligibilityModule.genderBased	AS gender_based,
        protocolSection.eligibilityModule.minimumAge	AS minimum_age,
        protocolSection.eligibilityModule.maximumAge	AS maximum_age,
        protocolSection.eligibilityModule.healthyVolunteers	AS healthy_volunteers,
        protocolSection.eligibilityModule.stdAges	AS std_ages,

        protocolSection.conditionsModule.conditions	AS conditions,
        protocolSection.conditionsModule.keywords	AS keywords,

        -- This will be turned into dimensions
        protocolSection.armsInterventionsModule.interventions	AS interventions,
        protocolSection.contactsLocationsModule.locations	AS locations,

        protocolSection.ipdSharingStatementModule.ipdSharing	AS ipd_sharing,

        hasResults	AS has_results

    FROM source
)
--Questions to anwers:
-- Trials by study types (study types)
-- Common conditions being studied (condiitons)
-- Interventions with the highest completion rate (types of interventions)
-- geographic distribution (country, etc)
-- Timeline analysis of study durations (duration)
SELECT * FROM flattened
