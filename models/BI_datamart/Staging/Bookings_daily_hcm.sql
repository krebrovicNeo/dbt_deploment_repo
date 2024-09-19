SELECT 'HCM'                                                                                                                                        as business_division
          , date(o.closedate)                                                                                                                            as reporting_date
          , a.id                                                                                                                                         as account_id
          , a.name                                                                                                                                       as account_name
          , a.territory__c                                                                                                                               as account_market
          , a.type                                                                                                                                       as account_type
          , a.billingstate                                                                                                                               as account_state
          , a.billingpostalcode                                                                                                                          as account_postal_code
          , a.segmentation_tier__c                                                                                                                       as segmentation_tier
          , a.agency_type__c                                                                                                                             as agency_type
          , o.id                                                                                                                                         as opportunity_id
          , CASE
                WHEN o.createddate < '2024-01-01'
                    THEN o.createddate
                ELSE o.appointment_date__c END                                                                                                           as opportunity_created_date

          , o.purchasing_vehicle__c                                                                                                                      as purchasing_vehicle
          , o.third_party_reseller__c                                                                                                                    as third_party_reseller
          , o.co_op_agreement__c                                                                                                                         as co_op_agreement

          , a.total_fte__c                                                                                                                               as total_ftes
          , a.became_a_customer_date__c                                                                                                                  as became_customer_date
          , m.mapped_productcode_value                                                                                                                   as product_code
          , li.productcode                                                                                                                               as product_code_raw
          , li.products_from_quote__c                                                                                                                    as item_name
          , li.products_from_quote__c                                                                                                                    as products_from_quote
          , li.producttype__c                                                                                                                               product_type

          , NULL::date                                                                                                                                      first_accred_association_date
          , NULL::date                                                                                                                                      first_accred_required_platform_date
          , NULL::int                                                                                                                                       is_accred_affiliated
          , NULL::boolean                                                                                                                                   is_partner_affiliated
          , NULL::text                                                                                                                                      accred_affiliation_ps

          , MIN(date(o.closedate))
            OVER (PARTITION BY a.id, m.mapped_productcode_value)                                                                                         as first_product_purchase_date
          , fsub.first_product_subscription_start_date

          , ROW_NUMBER()
            OVER (PARTITION BY o.id, li.productcode ,arr_bookings_amount, li.totalprice ORDER BY arr_bookings_amount, li.totalprice,li.createddate DESC) as index
          , NULL                                                                                                                                         as services_bookings_amount
          , CASE
                WHEN li.eligible_for_arr__c = 1 AND li.eligible_for_commission__c = 1
                    THEN li.totalprice
                ELSE 0 END                                                                                                                               as arr_bookings_amount

          , li.totalprice                                                                                                                                as total_bookings_amount

          , CASE
                WHEN o.closedate >= '20210101'
                    AND m.is_all_product_subscription = 1
                    AND li.eligible_for_arr__c = 1
                    AND li.eligible_for_commission__c = 1
                    THEN 1
                ELSE 0 END                                                                                                                               AS arr_bookings_units

          , CASE
                WHEN m.mapped_productcode_value IS NOT NULL AND m.mapped_productcode_value <> 'GJ' THEN 1
                ELSE 0 END                                                                                                                               as is_core_product
          , o.account_owner__c                                                                                                                           as owner_name
          , null                                                                                                                                         as referral_partner_content_provider
          , case
                when li.products_from_quote__c in
                     (select m.products_from_quote from mappings.v_ngv_opplineitem_product_to_partnerintegration m)
                    then 1
                else 0 end                                                                                                                                  is_partner_integration


     FROM sfdc_neogov_prod_v2.opportunitylineitem li
              INNER JOIN sfdc_neogov_prod_v2.opportunity o
                         ON li.opportunityid = o.id
              INNER JOIN sfdc_neogov_prod_v2.account a
                         ON o.accountid = a.id

              LEFT OUTER JOIN mappings.v_ngv_opportunitylineitem_productcode_to_coreproductcode m
                              ON m.raw_productcode_value = li.productcode
                                  AND m.mapped_productcode_value <> 'unmapped'

              LEFT OUTER JOIN (SELECT s.account_id,
                                      s.product_code,
                                      MIN(s.subscription_start_date) as first_product_subscription_start_date
                               FROM derived_datasets.v_account_product_subscriptions_neogov s
                               GROUP BY s.account_id, s.product_code) fsub
                              ON fsub.account_id = a.id
                                  AND fsub.product_code = m.mapped_productcode_value

     WHERE o.stagename = 'Closed - Won (Cust. Signature Obtained)'
       AND (o.original_opportunity_role__c IS NULL OR o.original_opportunity_role__c NOT ILIKE 'FNL%')
       AND o.isdeleted = 0
       AND li.isdeleted = 0