create table if not exists lmss_sb.dwmeggs_test
(
    case_number                           varchar(64),
    source                                varchar(4),
    status                                varchar(31),
    ip_address                            varchar(33),
    progress                              integer encode az64,
    duration_in_seconds                   integer encode az64,
    is_finished                           integer encode az64,
    response_id                           varchar(37),
    recipient_last_name                   varchar(49),
    recipient_first_name                  varchar(49),
    recipient_email                       varchar(49),
    external_reference                    varchar(49),
    lat                                   numeric(18) encode az64,
    lon                                   numeric(18) encode az64,
    distribution_channel                  varchar(19),
    language                              varchar(4),
    reviewer_name                         varchar(33),
    specialist_name                       varchar(33),
    origin                                varchar(45),
    action_bucket                       varchar(19),
    is_tracking_id_correct                varchar(6),
    is_resolution_code_correct            varchar(6),
    resolution_code                       varchar(46),
    incorrect_resolution_code             varchar(42),
    correct_resolution_code               varchar(42),
    is_code_reason_correct                varchar(6),
    incorrect_code_reason                 varchar(159),
    correct_code_reason                   varchar(159),
    is_action_correct                   varchar(6),
    incorrect_action                    varchar(114),
    correct_action                      varchar(123),
    is_evidence_type_correct              varchar(6),
    incorrect_evidence_type               varchar(438),
    correct_evidence_type                 varchar(537),
    is_description_accurate               varchar(6),
    is_contact_company_name_correct       varchar(6),
    is_transporter_id_correct             varchar(6),
    is_da_name_correct                    varchar(6),
    is_incident_date_correct              varchar(6),
    is_org_email_correct                  varchar(6),
    are_lmaq_comments_accurate            varchar(6),
    are_sds_comments_accurate             varchar(6),
    are_xyz_comments_accurate             varchar(6),
    is_email_to_org_correct               varchar(6),
    are_all_relevent_attachments_uploaded varchar(6),
    are_duplicates_properly_handled       varchar(40),
    is_appeal_rejection_accurate          varchar(6),
    teams_coaching_needed                 varchar(27),
    is_sop_update_recommended             varchar(6),
    parent_topics                         varchar(49),
    topics                                varchar(15),
    sentiment                             varchar(28),
    sentiment_score                       numeric(18) encode az64,
    sentiment_polarity                    numeric(18) encode az64,
    topic_sentiment_label                 varchar(49),
    topic_sentiment_score                 numeric(18) encode az64,
    error_type                            varchar(19),
    score                                 numeric(18) encode az64,
    date_started                          timestamp with time zone encode az64,
    date_ended                            timestamp with time zone encode az64,
    date_recorded                         timestamp with time zone encode az64,
    coaching_notes                        varchar(65535),
    reviewer_alias                        varchar(49),
    specialist_alias                      varchar(49),
    review_type                           varchar(100),
    is_customer_account_id_correct        varchar(49),
    critical_quality_score                numeric(8, 4) encode az64,
    perfect_quality_score                 numeric(8, 4) encode az64
)
    sortkey (source);


--DROP TABLE lmss_sb.dwmeggs_test;

GRANT ALL ON lmss_sb.dwmeggs_test TO cluster_rs_etl_team;

GRANT SELECT ON lmss_sb.dwmeggs_test TO org_team_qs_ro;

GRANT SELECT ON lmss_sb.dwmeggs_test TO GROUP team_users_dml;


---------------------------------------------------------------------------------------