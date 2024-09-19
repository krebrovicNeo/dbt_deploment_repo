SELECT row_number() over ()                                          as bookings_daily_id
     , combined.reporting_date
     , dc.week_end                                                   as reporting_week_end
     , dc.week_start                                                 as reporting_week_start
     , dc.working_day_of_month
     , dc.working_day_of_quarter
     , dc.working_day_of_year
     , dc.comp_date
     , dc.comp_week_end
     , dc.comp_week_start
     , dc.current_vs_previous
     , combined.business_division
     , combined.account_id
     , combined.account_name
     , combined.account_market
     , combined.account_type
     , agency_type
     , CASE
           WHEN combined.became_customer_date < combined.reporting_date THEN 'Existing'
           ELSE 'New' END                                            as business_type
     , combined.account_state
     , combined.account_postal_code
     , combined.segmentation_tier
     , combined.opportunity_id
     , combined.opportunity_created_date

     , combined.purchasing_vehicle
     , combined.third_party_reseller
     , combined.co_op_agreement

     , combined.total_ftes
     , combined.became_customer_date
     , combined.product_code
     , combined.product_code_raw
     , combined.item_name
     , combined.products_from_quote
     , combined.product_type
     , combined.first_accred_association_date
     , combined.first_accred_required_platform_date
     , combined.is_accred_affiliated
     , combined.accred_affiliation_ps
     , combined.first_product_purchase_date
     , combined.first_product_subscription_start_date
     , combined.total_bookings_amount
     , combined.arr_bookings_amount
     , combined.arr_bookings_units
     , combined.total_bookings_amount - combined.arr_bookings_amount as one_time_bookings_amount
     , combined.is_core_product
     , owner_name
     , referral_partner_content_provider
     , is_partner_integration
FROM (SELECT *
      FROM {{ ref('Bookings_Daily_pdms') }}
      UNION ALL
      SELECT *
      FROM {{ ref('Bookings_daily_hcm') }}
      ) as combined

         INNER JOIN derived_datasets.v_dates dc ON combined.reporting_date = dc.dt
WHERE 1 = 1
  and index = 1
  and account_name <> 'NEOGOV (CA)'
WITH NO SCHEMA BINDING;