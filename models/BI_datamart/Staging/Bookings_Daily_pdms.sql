SELECT 'PDMS'                                                                                                 as business_division
          , date(o.closedate)                                                                                      as reporting_date
          , a.id                                                                                                   as account_id
          , a.name                                                                                                 as account_name
          , a.industry                                                                                             as account_market
          , a.type                                                                                                 as account_type
          , a.shippingstate                                                                                        as account_state
          , a.shippingpostalcode                                                                                   as account_postal_code
          , NULL::text                                                                                             as segmentation_tier
          , null::text                                                                                             as agency_type
          , o.id                                                                                                   as opportunity_id
--            , CASE
--                  WHEN o.createddate < '2024-01-01'
--                      THEN o.createddate
--                  ELSE o.appointment_date__c END                                                                     as opportunity_created_date
          , NULL::date                                                                                             as opportunity_created_date

          , o.purchasing_vehicle__c                                                                                as purchasing_vehicle
          , o.third_party_reseller__c                                                                              as third_party_reseller
          , o.co_op_agreement__c                                                                                   as co_op_agreement

          , a.numberofemployees                                                                                    as total_ftes
          , a.date_became_customer__c                                                                              as became_customer_date
          , case
                when li.productcode in (
                                        'PDMSTrainingSolution',
                                        'PDMSTrainingSolutionCO',
                                        'PDMSTrainingSolutionHC',
                                        'PDMSTrainingSolutionPU',
                                        'PP-003',
                                        'PP-008HC',
                                        'PP-011CO'
                    ) then 'PowerTraining'
                else m.mapped_productfamily_value
            end                                                                                                    as product_code
          , li.productcode                                                                                         as product_code_raw
          , p.name                                                                                                 as item_name
          , li.product_family__c                                                                                   as products_from_quote
          , li.product_type__c                                                                                     as product_type
          , x.first_accred_association_date
          , x.first_accred_required_platform_date
          , x.is_accred_affiliated
          , x.is_partner_affiliated
          , x.accred_affiliation_ps

          , MIN(date(o.closedate))
            OVER (PARTITION BY a.id, m.mapped_productfamily_value)                                                 as first_product_purchase_date
          , fsub.first_product_subscription_start_date

          , DENSE_RANK()
            OVER (PARTITION BY o.id, li.productcode ORDER BY li.calculate_arr_formula__c DESC, li.enddate__c DESC) as index

          , CASE WHEN li.product_type__c = 'Services' THEN li.totalprice ELSE 0 END                                as services_bookings_amount

          , CASE

                WHEN o.closedate < '08/01/2021' THEN
                    li.arr__c

                WHEN o.closedate >= '01/01/2022' THEN
                    li.commissionable_arr_rep__c -- New Logic 06/06/2023

                ELSE
                    CASE
                        WHEN li.calculate_arr_formula__c = 1 AND
                             ISNULL(li.sbqq__subscriptiontype__c, '') <> 'One-Time' THEN li.line_arr__c
                        ELSE 0 END
            END                                                                                                    as arr_bookings_amount

          , CASE
                WHEN o.closedate < '08/01/2021' THEN li.totalprice
                ELSE
                    CASE
                        WHEN o.closedate >= '01/01/2022' AND li.product_type__c = 'Services' THEN li.totalprice
                        ELSE
                            CASE
                                WHEN o.closedate >= '01/01/2022'
                                    THEN li.commissionable_arr_rep__c -- New Logic 06/06/2023

                                ELSE
                                    CASE
                                        WHEN li.product_type__c = 'Services' THEN li.totalprice
                                        ELSE
                                            CASE
                                                WHEN li.calculate_arr_formula__c = 1 AND
                                                     ISNULL(li.sbqq__subscriptiontype__c, '') <> 'One-Time'
                                                    THEN li.line_arr__c
                                                ELSE 0 END
                                        END
                                END
                        END
            END                                                                                                    as total_bookings_amount
          , CASE
                WHEN COALESCE(li.commissionable_arr_rep__c, li.line_arr__c, li.arr__c) < 0 THEN -1
                WHEN li.product_family__c IN ('Engage',
                                              'PowerDMS',
                                              'PowerFTO',
                                              'PowerReady',
                                              'PowerLine',
                                              'PowerSTANDARDS',
                                              'PowerTime',
                                              'Professional',
                                              'Standards', 'Training', 'Select',
                                              'Power Professional Standards',
                                              'Recall',
                                              'Vetted',
                                              'Schedule')
                    AND li.isdeleted = 0
                    AND p.isdeleted = 0
                    AND p.product_type__c = 'Recurring'
                    AND ((li.commissionable_arr_rep__c > 0) or (li.line_arr__c > 0) or (li.arr__c > 0))
                    AND (o.closedate < '08/01/2021' OR li.calculate_arr_formula__c = 1)
                    AND ((fsub.first_product_subscription_start_date >= DATEADD('day', -90, o.closedate))
                        AND li.upgraded_subscription__c is null)
                    AND ROW_NUMBER() OVER (PARTITION BY o.closedate, o.accountid, li.productcode
                        ORDER BY li.commissionable_arr_rep__c DESC, li.line_arr__c DESC, li.arr__c DESC, o.closedate DESC) =
                        1 -- only attribute 1 unit per product/account/date
                    THEN 1
                ELSE 0 END
                                                                                                                   as arr_bookings_units

          , CASE
                WHEN (li.product_family__c IN ('Engage',
                                               'PowerDMS',
                                               'PowerFTO',
                                               'PowerReady',
                                               'PowerLine',
                                               'PowerTime',
                                               'PowerLine',
                                               'Professional',
                                               'Recall',
                                               'Vetted',
                                               'Schedule') OR
                      (li.product_family__c = 'Power Professional Standards' AND li.is_core_product__c = 1))-- Standards not considered core product
                    AND p.product_type__c = 'Recurring'
                    AND ((li.commissionable_arr_rep__c > 0) or (li.line_arr__c > 0) or (li.arr__c > 0))
                    AND (li.createddate < '08/01/2021' OR li.calculate_arr_formula__c = 1)
                    THEN 1
                ELSE 0 END                                                                                         as is_core_product
          , o.owner_name__c                                                                                        as owner_name
          , a.referral_partner_content_provider__c                                                                 as referral_partner_content_provider

          , case
                when li.product_family__c in
                     (select m.products_from_quote from mappings.v_ngv_opplineitem_product_to_partnerintegration m)
                    then 1
                else 0 end                                                                                            is_partner_integration

     FROM sfdc_silver.mv_opportunitylineitem li
              INNER JOIN mappings.v_pdms_opplineitem_productfamily_to_productgroup m
                         ON li.product_family__c = m.raw_productfamily_value
              INNER JOIN sfdc_silver.mv_opportunity o
                         ON li.opportunityid = o.id
              INNER JOIN sfdc_silver.mv_account a
                         ON o.accountid = a.id

              LEFT OUTER JOIN derived_datasets.v_pdms_account_partnership_attributes x
                              ON x.account_id = a.id

              LEFT OUTER JOIN {{ ref('Product2') }} p
                              ON li.product2id = p.id
                                  AND p.isdeleted = 0

              LEFT OUTER JOIN (SELECT s.account_id,
                                      s.product_code,
                                      MIN(s.subscription_start_date) as first_product_subscription_start_date
                               FROM derived_datasets.v_account_product_subscriptions_pdms s
                               GROUP BY s.account_id, s.product_code) fsub
                              ON fsub.account_id = a.id
                                  AND fsub.product_code = m.mapped_productfamily_value


     WHERE o.stagename = 'Closed Won'
       AND o.isdeleted = 0
       AND li.isdeleted = 0
       AND (o.type IS NULL OR NOT (o."type"::text IN ('Migration Target', 'Renewal'))) -- New Logic
       AND not (
             (o.contract_1st_year__c = 0 and o.opp1styrrevenue_master__c = 0) -- ignore opp line items where the parent opportunity has no revenue associated with it; we found some anomalies like this.
             or (
                 o.contract_1st_year__c is null and o.opp1styrrevenue_master__c is null
                 )
         )