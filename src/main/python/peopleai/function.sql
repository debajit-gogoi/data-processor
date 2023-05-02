SELECT    cme.uid                  AS "uid",
          a.id                     AS "account_id",
          a.NAME                   AS "account_name",
          a.type                   AS "account_type",
          a.ownerid                AS "account_owner_id",
          a.createddate            AS "account_created_date",
          a.account_theater__c     AS "account_theater",
          a.account_sub_theater__c AS "account_region",
          a.account_sub_region__c  AS "account_subregion",
          au.NAME "account_owner_name",
          a.account_record_type__c  AS "account_record_type__c",
          a.account_segmentation__c AS "account_segmentation__c",
          a.tam_source__c           AS "tam_source__c",
          a.assets__c               AS "debajit_tam",
          a.debajit_tam_override__c ,
          CASE
                    WHEN a.debajit_tam_override__c IS NULL THEN a.assets__c
                    WHEN a.debajit_tam_override__c=0 THEN a.assets__c
                    ELSE a.debajit_tam_override__c
          END AS "Final_TAM",
          a.industry,
          a.company_size__c,
          a.vertical__c,
          a.infer_score__c,
          a.annual_revenue__c,
          a.infer_rating__c,
          a.total_pipeline__c,
          a.account_owner_job_role__c,
          u.id                                     AS "rep_id",
          u.NAME                                   AS "rep_name",
          u.username                               AS "rep_email",
          u.job_role__c                            AS "user_role",
          u.isactive                               AS user_isactive,
          cme.request_type                         AS "request_type",
          cme.event_timestamp                      AS "event_timestamp",
          (cme.event_timestamp at time zone 'utc') AS event_timestamp_utc,
          cme.status                               AS "status",
          cme.tags                                 AS "tags",
          cme.direction                            AS "direction",
          pap.email                                AS "participant_email",
          pap.NAME                                 AS "participant_name",
          pap.type                                 AS "participant_type",
          pap.time_spent                           AS "time_spent",
          -- (select count(*) from
          -- peopleai.people_ai_participants pap2 where pap2.uid=cme.uid and pap2.type='internal') internal_participant_count ,
          cme.duration AS "duration",
          CASE
                    WHEN cme.request_type='meeting' THEN cme.duration
                    ELSE pap.time_spent
          END actual_time_spent_in_sec
FROM      peopleai.people_ai_call_meeting_email cme
JOIN      peopleai.people_ai_crmstatus crms
ON        cme.uid=crms.uid
JOIN      peopleai.people_ai_participants pap
ON        pap.uid=cme.uid
          -- join sfdc.opportunity op on op.accountid=crms.acc_crm_id
JOIN      sfdc.account a
ON        a.id=crms.acc_crm_id
LEFT JOIN sfdc.user2 u
ON        u.username=pap.email -- and u.isactive = true
LEFT JOIN sfdc.user2 au
ON        au.id=a.ownerid
WHERE     cme.event_timestamp >= make_date(cast(extract(year FROM ((CURRENT_DATE-interval '1 year') + interval '5 month'))-1 AS int),8,1)
AND       pap.type = 'internal'