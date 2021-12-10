# do not execute this sql
# *use Makefile

CREATE DATABASE IF NOT EXISTS `analytics`;

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.dnb` (
    `type` string COMMENT '',
    `duns_id` integer COMMENT '',
    `company_name` string COMMENT '',
    `full_address` string COMMENT '',
    `street_address` string COMMENT '',
    `city` string COMMENT '',
    `state` string COMMENT '',
    `postal_code` string COMMENT '',
    `country` string COMMENT '',
    `url` string COMMENT '',
    `parent_url` string COMMENT '',
    `revenue` float COMMENT '',
    `employees` integer COMMENT '',
    `description` string COMMENT '',
    `entity_type` string COMMENT '',
    `is_parent` boolean COMMENT '',
    `parent_uid` string COMMENT '',
    `parent_duns_id` float COMMENT '',
    `parent_company_name` string COMMENT '',
    `parent_country` string COMMENT '',
    `industry` string COMMENT '',
    `company_size_type` string COMMENT '',
    `company_size_range` string COMMENT '',
    `parent_sites` integer COMMENT '',
    `parent_site_range` string COMMENT '',
    `parent_employees` integer COMMENT '',
    `parent_company_size_type` string COMMENT '',
    `parent_company_size_range` string COMMENT '',
    `domain` string COMMENT '',
    `parent_domain` string COMMENT '',
    `google_place_id` string COMMENT '',
    `latitude` float COMMENT '',
    `longitude` float COMMENT '',
    `lat_bin` float COMMENT '',
    `long_bin` float COMMENT '',
    `LatLongID` string COMMENT '',
    `postcode` integer COMMENT '',
    `account_id` string COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/dnb/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.sfdc_geocode` (
    `account_id` string COMMENT '',
    `accuracy` string COMMENT '',
    `formatted_address` string COMMENT '',
    `google_place_id` string COMMENT '',
    `latitude` float COMMENT '',
    `longitude` float COMMENT '',
    `lat_bin` float COMMENT '',
    `long_bin` float COMMENT '',
    `LatLongID` string COMMENT '',
    `postcode` string COMMENT '',
    `geocode_status` string COMMENT '',
    `geocode_type` string COMMENT '',
    `location_confidence` string COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/sfdc_geocode/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gym_attributes` (
    `gym_id` integer COMMENT '',
    `women_gyms` boolean COMMENT '',
    `family_gym` boolean COMMENT '',
    `training_center` boolean COMMENT '',
    `sports_club` boolean COMMENT '',
    `wellness_center` boolean COMMENT '',
    `offers_classes` boolean COMMENT '',
    `gym_network_type` string COMMENT '',
    `gym_network_geo` string COMMENT '',
    `gym_type` string COMMENT '',
    `category_level_1` string COMMENT '',
    `category_level_2` string COMMENT '',
    `workout_type` string COMMENT '',
    `difficulty` string COMMENT '',
    `location_group` string COMMENT '',
    `gym_group` string COMMENT '',
    `google_place_id` string COMMENT '',
    `google_closed` string COMMENT '',
    `google_name` string COMMENT '',
    `google_price_level` float COMMENT '',
    `google_rating` float COMMENT '',
    `google_rating_total` float COMMENT '',
    `google_type` string COMMENT '',
    `google_status` string COMMENT '',
    `account_id` string COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gym_attributes/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.sfdc_crm_crosswalk` (
    `company_id` integer COMMENT '',
    `account_id` string COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/sfdc_crm_crosswalk/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.activation_targets` (
    `company_size_range` string COMMENT '',
    `business_type` string COMMENT '',
    `dependant_flag` boolean COMMENT '',
    `payroll_enabled` boolean COMMENT '',
    `country_title` string COMMENT '',
    `breakdown` string COMMENT '',
    `time_btwn` integer COMMENT '',
    `sign_up_rate` float COMMENT '',
    `enrollment_rate` float COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/activation_targets/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.salesforce_account_owner` (
    `salesforce_id` string COMMENT '',
    `user_id` string COMMENT '',
    `month` integer COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/salesforce_account_owner/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.salesforce_account_quota` (
    `salesforce_id` string COMMENT '',
    `currency_title` string COMMENT '',
    `month` integer COMMENT '',
    `type` string COMMENT '',
    `quota` float COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/salesforce_account_quota/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.salesforce_user_quota` (
    `user_id` string COMMENT '',
    `currency_title` string COMMENT '',
    `month` integer COMMENT '',
    `type` string COMMENT '',
    `quota` float COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/salesforce_user_quota/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.minimum_access_fee` (
    `company_size_range` string COMMENT '',
    `country_title` string COMMENT '',
    `month` integer COMMENT '',
    `minimum_access_fee` float COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/minimum_access_fee/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.salesforce_user_roster` (
    `user_id` string COMMENT '',
    `user_role_id` string COMMENT '',
    `user_manager_id` string COMMENT '',
    `month` integer COMMENT '',
    `start_date` integer COMMENT '',
    `role_start_date` integer COMMENT '',
    `commission_start_date` integer COMMENT '',
    `termination_date` integer COMMENT '',
    `user_role_level` integer COMMENT '',
    `currency_title` string COMMENT '',
    `quota_percentage` float COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/salesforce_user_roster/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.active_sales` (
    `date` integer COMMENT '',
    `country` string COMMENT '',
    `campaign` string COMMENT '',
    `person_id` integer COMMENT '',
    `phone_number` integer COMMENT '',
    `call_outcome` string COMMENT '',
    `call_duration` string COMMENT '',
    `operater` string COMMENT '',
    `gympass_login` string COMMENT '',
    `source` string COMMENT '',
    `result` string COMMENT '',
    `answered_call` boolean COMMENT '',
    `correct_person_answered` boolean COMMENT '',
    `success` boolean COMMENT '',
    `try_again` boolean COMMENT '',
    `no_call` boolean COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/active_sales/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.anaplan` (
    `version` string COMMENT '',
    `country_l3` string COMMENT '',
    `b2b_smb` string COMMENT '',
    `s_currency_selection_local_currency_fx_neutral` string COMMENT '',
    `period` string COMMENT '',
    `license_fees` decimal(12, 2) COMMENT '',
    `membership_fees_b2b` decimal(12, 2) COMMENT '',
    `discounts_and_refunds_b2b` decimal(12, 2) COMMENT '',
    `b2c` decimal(12, 2) COMMENT '',
    `discounts_and_refunds_b2c` decimal(12, 2) COMMENT '',
    `setup_fee` decimal(12, 2) COMMENT '',
    `other_sales` decimal(12, 2) COMMENT '',
    `total_sales` decimal(12, 2) COMMENT '',
    `sales_growth` decimal(12, 2) COMMENT '',
    `lf_growth` decimal(12, 2) COMMENT '',
    `mf_growth` decimal(12, 2) COMMENT '',
    `gym_payments_b2b` decimal(12, 2) COMMENT '',
    `gym_payments_b2c` decimal(12, 2) COMMENT '',
    `gym_payments` decimal(12, 2) COMMENT '',
    `gym_payments_as_perc_of_sales` decimal(12, 2) COMMENT '',
    `revenue_b2b` decimal(12, 2) COMMENT '',
    `revenue_b2b_as_perc_of_sales` decimal(12, 2) COMMENT '',
    `revenue_b2c` decimal(12, 2) COMMENT '',
    `revenue_b2c_as_perc_of_sales` decimal(12, 2) COMMENT '',
    `revenue` decimal(12, 2) COMMENT '',
    `revenue_as_perc_of_sales` decimal(12, 2) COMMENT '',
    `existing_eligible` decimal(12, 2) COMMENT '',
    `new_eligible` decimal(12, 2) COMMENT '',
    `eligibles` decimal(12, 2) COMMENT '',
    `eligible_growth` decimal(12, 2) COMMENT '',
    `existing_enrolled` decimal(12, 2) COMMENT '',
    `new_enrolled` decimal(12, 2) COMMENT '',
    `enrolled` decimal(12, 2) COMMENT '',
    `enrolled_growth` decimal(12, 2) COMMENT '',
    `enrollment_rate_existing` decimal(12, 2) COMMENT '',
    `enrollment_rate_new` decimal(12, 2) COMMENT '',
    `enrollment_rate` decimal(12, 2) COMMENT '',
    `free_as_perc_of_total_enrolled` decimal(12, 2) COMMENT '',
    `perc_of_cancelled` decimal(12, 2) COMMENT '',
    `paying_purchasers_existing` decimal(12, 2) COMMENT '',
    `paying_purchasers_new` decimal(12, 2) COMMENT '',
    `paying_purchasers` decimal(12, 2) COMMENT '',
    `purchasers` decimal(12, 2) COMMENT '',
    `lf_per_eligible_existing` decimal(12, 2) COMMENT '',
    `lf_per_eligible_new` decimal(12, 2) COMMENT '',
    `lf_per_eligible` decimal(12, 2) COMMENT '',
    `lf_per_enrolled_existing` decimal(12, 2) COMMENT '',
    `lf_per_enrolled_new` decimal(12, 2) COMMENT '',
    `lf_per_enrolled` decimal(12, 2) COMMENT '',
    `avg_membership_fee_existing` decimal(12, 2) COMMENT '',
    `avg_membership_fee_new` decimal(12, 2) COMMENT '',
    `avg_membership_perr_enrolled` decimal(12, 2) COMMENT '',
    `avg_mf_per_paying_enrolled_existing` decimal(12, 2) COMMENT '',
    `avg_mf_per_paying_enrolled_new` decimal(12, 2) COMMENT '',
    `avg_membership_per_paying_enrolled` decimal(12, 2) COMMENT '',
    `avg_gym_pmt_per_enrolled` decimal(12, 2) COMMENT '',
    `mf_revenue_b2b_only` decimal(12, 2) COMMENT '',
    `mf_margin` decimal(12, 2) COMMENT '',
    `perc_active_users` decimal(12, 2) COMMENT '',
    `frequency` decimal(12, 2) COMMENT '',
    `gym_visits` decimal(12, 2) COMMENT '',
    `visits_per_enrolled` decimal(12, 2) COMMENT '',
    `cost_per_visits` decimal(12, 2) COMMENT '',
    `avg_net_mf_per_purchaser` decimal(12, 2) COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/anaplan/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.edw_transaction_fixes` (
    `data_source` string COMMENT '',
    `person_cart_id` integer COMMENT '',
    `person_id` integer COMMENT '',
    `purchase_person_id` integer COMMENT '',
    `company_id` integer COMMENT '',
    `date` integer COMMENT '',
    `date_hour` integer COMMENT '',
    `refund_date` integer COMMENT '',
    `action` string COMMENT '',
    `gym_id` integer COMMENT '',
    `country_id` integer COMMENT '',
    `payment_method` string COMMENT '',
    `payment_type` string COMMENT '',
    `enrollment_type` string COMMENT '',
    `gym_product_value` decimal(12, 2) COMMENT '',
    `token_group_type` string COMMENT '',
    `active_plan_value` decimal(12, 2) COMMENT '',
    `sales_value` decimal(12, 2) COMMENT '',
    `revenue_value` decimal(12, 2) COMMENT '',
    `gym_expenses_value` decimal(12, 2) COMMENT '',
    `refund_value` decimal(12, 2) COMMENT '',
    `valid_start_day` integer COMMENT '',
    `valid_end_day` integer COMMENT '',
    `valid_end_day_adjusted` integer COMMENT '',
    `valid_days_curr_month` integer COMMENT '',
    `valid_days_next_month` integer COMMENT '',
    `total_price` decimal(12, 2) COMMENT '',
    `partner_visit_action` string COMMENT '',
    `manual_inputs_description` string COMMENT '',
    `original_adjustment_date` integer COMMENT '',
    `adjustment_comments` string COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/edw_transaction_fixes/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.location_attributes` (
    `latlongid` string COMMENT 'latlongid',
    `uid` string COMMENT 'uid',
    `gid_0` string COMMENT 'gid_0',
    `country` string COMMENT 'country',
    `gid_1` string COMMENT 'gid_1',
    `state_prov` string COMMENT 'state_prov',
    `type_1` string COMMENT 'type_1',
    `gid_2` string COMMENT 'gid_2',
    `county_city` string COMMENT 'county_city',
    `type_2` string COMMENT 'type_2',
    `gid_3` string COMMENT 'gid_3',
    `district` string COMMENT 'district',
    `type_3` string COMMENT 'type_3',
    `gid_4` string COMMENT 'gid_4',
    `sublocality` string COMMENT 'sublocality',
    `type_4` string COMMENT 'type_4',
    `gid_5` string COMMENT 'gid_5',
    `neighborhood` string COMMENT 'neighborhood',
    `type_5` string COMMENT 'type_5',
    `zone` float COMMENT 'zone'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/location_attributes/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.postal_codes` (
    `postal_code` string COMMENT 'postal_code',
    `country_id` integer COMMENT 'country_id',
    `country` string COMMENT 'country',
    `country_code` string COMMENT 'country_code',
    `country_region_group` string COMMENT 'country_region_group',
    `city` string COMMENT 'city',
    `state` string COMMENT 'state',
    `state_code` string COMMENT 'state_code',
    `county_province` string COMMENT 'county_province',
    `county_province_code` string COMMENT 'county_province_code',
    `community` string COMMENT 'community',
    `community_code` string COMMENT 'community_code',
    `latitude` float COMMENT 'latitude',
    `longitude` float COMMENT 'longitude',
    `accuracy` string COMMENT 'accuracy'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/postal_codes/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.client_sites` (
    `salesforce_id` string COMMENT 'salesforce_id',
    `input_address` string COMMENT 'address',
    `site_employees` integer COMMENT 'site_employees',
    `is_hq` boolean COMMENT 'is_hq',
    `formatted_address` string COMMENT 'formatted_address',
    `neighborhood` string COMMENT 'neighborhood',
    `city` string COMMENT 'city',
    `county` string COMMENT 'county',
    `state` string COMMENT 'state',
    `postal_code` string COMMENT 'postal_code',
    `country` string COMMENT 'country',
    `latitude` float COMMENT 'latitude',
    `longitude` float COMMENT 'longitude',
    `accuracy` string COMMENT 'accuracy',
    `google_place_id` string COMMENT 'google_place_id',
    `type` string COMMENT 'type',
    `number_of_results` integer COMMENT 'type',
    `status` string COMMENT 'type'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/client_sites/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gw_user` (
  `id` string COMMENT 'id',
  `location_id` integer COMMENT 'location_id',
  `promo_code_id` integer COMMENT 'promo_code_id',
  `email` string COMMENT 'email',
  `name` string COMMENT 'name',
  `company` string COMMENT 'company',
  `location` string COMMENT 'location',
  `plan` string COMMENT 'plan',
  `pp_order_id` string COMMENT 'pp_order_id',
  `pp_subscription_id` string COMMENT 'pp_subscription_id',
  `pp_facilitator_access_token` string COMMENT 'pp_facilitator_access_token',
  `neou_link` string COMMENT 'neou_link',
  `tecnonutri_link` string COMMENT 'tecnonutri_link',
  `zenapp_link` string COMMENT 'zenapp_link',
  `eightfit_link` string COMMENT 'eightfit_link',
  `utm_campaign` string COMMENT 'utm_compaing',
  `active_user` smallint COMMENT 'active_user',
  `created_at` string COMMENT 'created_at',
  `updated_at` string COMMENT 'updated_at'
  )
  COMMENT ''
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gw_user/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gw_location` (
  `id` integer COMMENT 'id',
  `country_code` string COMMENT 'country_code',
  `name` string COMMENT 'name',
  `email_template_id` string COMMENT 'email_template_id'
  )
COMMENT ''
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gw_location/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gw_promo_code` (
  `id` integer COMMENT 'id',
  `code` string COMMENT 'code',
  `name` string COMMENT 'name',
  `max_usage` integer COMMENT 'max_usage',
  `issued_for` string COMMENT 'issued_for',
  `active` smallint COMMENT 'active',
  `created_at` string COMMENT 'created_at',
  `updated_at` string COMMENT 'updated_at'
 )
COMMENT ''
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gw_promo_code/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gw_promo_code_has_location` (
  `location_id` integer COMMENT 'location_id',
  `promo_code_id` integer COMMENT 'promo_code_id'
  )
COMMENT ''
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gw_promo_code_has_location/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.churn_accum_plot` (
    `perc_pop_accum` float COMMENT 'perc_pop_accum',
    `perc_y_true_accum` float COMMENT 'perc_y_true_accum',
    `TS_DAY` date COMMENT 'TS_DAY',
    `TS_EXECUTION` date COMMENT 'TS_EXECUTION',
    `REGION` string COMMENT 'REGION'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/churn_accum_plot/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.churn_auc` (
    `auc` float COMMENT 'auc',
    `TS_DAY` date COMMENT 'TS_DAY',
    `TS_EXECUTION` date COMMENT 'TS_EXECUTION',
    `REGION` string COMMENT 'REGION'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/churn_auc/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.churn_cluster_centroids` (
    `CURRENT_GYM_NON_VISIT_STREAK_median` integer COMMENT 'CURRENT_GYM_NON_VISIT_STREAK_median',
    `CHECK_IN_COUNT_median` float COMMENT 'CHECK_IN_COUNT_median',
    `HISTORICAL_VISITS_median` integer COMMENT 'HISTORICAL_VISITS_median',
    `CURRENT_PAYMENT_SEQUENCE_COUNT_median` integer COMMENT 'CURRENT_PAYMENT_SEQUENCE_COUNT_median',
    `PAID_TO_PLAN_PRICE_RATIO_median` float COMMENT 'PAID_TO_PLAN_PRICE_RATIO_median',
    `PLAN_PRICE_median` float COMMENT 'PLAN_PRICE_median',
    `COMMUNICATION_CHANNEL_all_30D_median` integer COMMENT 'COMMUNICATION_CHANNEL_all_30D_median',
    `APP_USAGE_30D_median` float COMMENT 'APP_USAGE_30D_median',
    `APP_SEARCH_30D_median` integer COMMENT 'APP_SEARCH_30D_median',
    `WEB_USAGE_RATIO_30D_median` float COMMENT 'WEB_USAGE_RATIO_30D_median',
    `CHURN_COUNT` float COMMENT 'CHURN_COUNT',
    `CHURN_RATIO` float COMMENT 'CHURN_RATIO',
    `USER_COUNT` integer COMMENT 'USER_COUNT',
    `USER_REPRESENT` float COMMENT 'USER_REPRESENT',
    `CHURN_REPRESENT` float COMMENT 'CHURN_REPRESENT'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/churn_cluster_centroids/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.churn_conf_matrix_plot` (
    `TS_DAY` date COMMENT 'TS_DAY',
    `false_negative` float COMMENT 'false_negative',
    `false_positive` float COMMENT 'false_positive',
    `perc_pop_accum` float COMMENT 'perc_pop_accum',
    `precision` float COMMENT 'precision',
    `recall` float COMMENT 'recall',
    `true_negative` float COMMENT 'true_negative',
    `true_positive` float COMMENT 'true_positive',
    `TS_EXECUTION` date COMMENT 'TS_EXECUTION',
    `REGION` string COMMENT 'REGION'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/churn_conf_matrix_plot/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.churn_feat_import` (
    `feature_name` string COMMENT 'feature_name',
    `importance` float COMMENT 'importance',
    `REGION` string COMMENT 'REGION'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/churn_feat_import/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.churn_lift_plot` (
    `perc_pop_accum` float COMMENT 'perc_pop_accum',
    `lift` float COMMENT 'lift',
    `TS_DAY` date COMMENT 'TS_DAY',
    `TS_EXECUTION` date COMMENT 'TS_EXECUTION',
    `REGION` string COMMENT 'REGION'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/churn_lift_plot/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.churn_roc_plot` (
    `fpr` float COMMENT 'fpr',
    `tpr` float COMMENT 'tpr',
    `TS_DAY` date COMMENT 'TS_DAY',
    `TS_EXECUTION` date COMMENT 'TS_EXECUTION',
    `REGION` string COMMENT 'REGION'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/churn_roc_plot/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.churn_mt_scored_clustered_valued` (
    `TS_DAY` date COMMENT 'TS_DAY',
    `IS_CHURN_IN_D30` integer COMMENT 'IS_CHURN_IN_D30',
    `IS_DISMISSED_D30_BEFORE_TO_D30_AFTER` integer COMMENT 'IS_DISMISSED_D30_BEFORE_TO_D30_AFTER',
    `IS_CHURN_VOL_IN_D30` integer COMMENT 'IS_CHURN_VOL_IN_D30',
    `country_id` float COMMENT 'country_id',
    `DAYS_SINCE_FIRST_CREATION` float COMMENT 'DAYS_SINCE_FIRST_CREATION',
    `DAYS_SINCE_FIRST_CONFIRMATION` float COMMENT 'DAYS_SINCE_FIRST_CONFIRMATION',
    `DAYS_SINCE_FIRST_SIGNIN` float COMMENT 'DAYS_SINCE_FIRST_SIGNIN',
    `DAYS_SINCE_FIRST_PAYMENT` float COMMENT 'DAYS_SINCE_FIRST_PAYMENT',
    `DAYS_SINCE_FIRST_GYM_VISIT` float COMMENT 'DAYS_SINCE_FIRST_GYM_VISIT',
    `DAYS_FROM_CREATION_TO_SIGNIN` float COMMENT 'DAYS_FROM_CREATION_TO_SIGNIN',
    `DAYS_FROM_CREATION_TO_PAYMENT` float COMMENT 'DAYS_FROM_CREATION_TO_PAYMENT',
    `DAYS_FROM_SIGNIN_TO_PAYMENT` float COMMENT 'DAYS_FROM_SIGNIN_TO_PAYMENT',
    `DAYS_FROM_PAYMENT_TO_GYM_VISIT` float COMMENT 'DAYS_FROM_PAYMENT_TO_GYM_VISIT',
    `GAP_SIZE_SUM_HISTORICAL` integer COMMENT 'GAP_SIZE_SUM_HISTORICAL',
    `GAP_SIZE_MAX_HISTORICAL` integer COMMENT 'GAP_SIZE_MAX_HISTORICAL',
    `GAP_COUNT_SUM_HISTORICAL` integer COMMENT 'GAP_COUNT_SUM_HISTORICAL',
    `GAP_COUNT_SUM_30D` integer COMMENT 'GAP_COUNT_SUM_30D',
    `NUM_FAMILY_MEMBERS` float COMMENT 'NUM_FAMILY_MEMBERS',
    `IS_ACCOUNT_HOLDER` float COMMENT 'IS_ACCOUNT_HOLDER',
    `user_company_id` integer COMMENT 'user_company_id',
    `user_parent_company_id` integer COMMENT 'user_parent_company_id',
    `PAID_PRICE` float COMMENT 'PAID_PRICE',
    `PLAN_PRICE` float COMMENT 'PLAN_PRICE',
    `CURRENT_PAYMENT_SEQUENCE_COUNT` integer COMMENT 'CURRENT_PAYMENT_SEQUENCE_COUNT',
    `PAID_TO_PLAN_PRICE_RATIO` float COMMENT 'PAID_TO_PLAN_PRICE_RATIO',
    `PAYMENT_METHOD_AdyenIdealPayment` integer COMMENT 'PAYMENT_METHOD_AdyenIdealPayment',
    `PAYMENT_METHOD_RemotePersonCard` integer COMMENT 'PAYMENT_METHOD_RemotePersonCard',
    `PAYMENT_METHOD_Payroll` integer COMMENT 'PAYMENT_METHOD_Payroll',
    `PAYMENT_METHOD_BankAccount` integer COMMENT 'PAYMENT_METHOD_BankAccount',
    `PAYMENT_METHOD_PaypalAgreement` integer COMMENT 'PAYMENT_METHOD_PaypalAgreement',
    `PURCHASE_TYPE_Primeira_compra` integer COMMENT 'PURCHASE_TYPE_Primeira_compra',
    `PURCHASE_TYPE_Renovacao_de_plano` integer COMMENT 'PURCHASE_TYPE_Renovacao_de_plano',
    `COMPANY_TYPE_Employees` integer COMMENT 'COMPANY_TYPE_Employees',
    `COMPANY_TYPE_Former_Members` integer COMMENT 'COMPANY_TYPE_Former_Members',
    `COMPANY_TYPE_Family_Member` integer COMMENT 'COMPANY_TYPE_Family_Member',
    `ACQUISITION_CHANNEL_B2B` integer COMMENT 'ACQUISITION_CHANNEL_B2B',
    `ACQUISITION_CHANNEL_B2C` integer COMMENT 'ACQUISITION_CHANNEL_B2C',
    `IS_D2_PAYMENT_FAILURE` integer COMMENT 'IS_D2_PAYMENT_FAILURE',
    `N_OF_PAYMENT_FAILURE_LAST_30D` integer COMMENT 'N_OF_PAYMENT_FAILURE_LAST_30D',
    `N_OF_IS_D2_PAYMENT_FAILURE_LAST_30D` integer COMMENT 'N_OF_IS_D2_PAYMENT_FAILURE_LAST_30D',
    `TOTAL_REFUND` float COMMENT 'TOTAL_REFUND',
    `DELTA_DAYS_REFUND_TO_VALID_END` float COMMENT 'DELTA_DAYS_REFUND_TO_VALID_END',
    `RATIO_REFUND_TO_PAID` float COMMENT 'RATIO_REFUND_TO_PAID',
    `REFUNDED_SAME_DAY_OR_NEXT` integer COMMENT 'REFUNDED_SAME_DAY_OR_NEXT',
    `D30_BEFORE_TO_CURRENT_PAID_PRICE_RATIO` float COMMENT 'D30_BEFORE_TO_CURRENT_PAID_PRICE_RATIO',
    `D30_BEFORE_TO_CURRENT_PLAN_PRICE_RATIO` float COMMENT 'D30_BEFORE_TO_CURRENT_PLAN_PRICE_RATIO',
    `D30_BEFORE_TO_CURRENT_PAID_TO_PLAN_PRICE_RATIO` float COMMENT 'D30_BEFORE_TO_CURRENT_PAID_TO_PLAN_PRICE_RATIO',
    `IS_BILL_SHOCK_30D` float COMMENT 'IS_BILL_SHOCK_30D',
    `BILL_SHOCK_INCREASE_RATIO_30D` float COMMENT 'BILL_SHOCK_INCREASE_RATIO_30D',
    `CARD_EXPIRATION_NEXT_30D` float COMMENT 'CARD_EXPIRATION_NEXT_30D',
    `CARD_EXPIRATION_NEXT_45D` float COMMENT 'CARD_EXPIRATION_NEXT_45D',
    `CARD_EXPIRATION_NEXT_60D` float COMMENT 'CARD_EXPIRATION_NEXT_60D',
    `N_ELIGIBLE_MEMBERS` integer COMMENT 'N_ELIGIBLE_MEMBERS',
    `N_ENROLLED_MEMBERS` integer COMMENT 'N_ENROLLED_MEMBERS',
    `MEMBER_ENROLLMENT_RATIO` float COMMENT 'MEMBER_ENROLLMENT_RATIO',
    `N_ENROLLED_MEMBERS_PCT_CHANGE` float COMMENT 'N_ENROLLED_MEMBERS_PCT_CHANGE',
    `N_ELIGIBLE_MEMBERS_PCT_CHANGE` float COMMENT 'N_ELIGIBLE_MEMBERS_PCT_CHANGE',
    `MEMBER_ENROLLMENT_RATIO_PCT_CHANGE` float COMMENT 'MEMBER_ENROLLMENT_RATIO_PCT_CHANGE',
    `N_ENROLLED_MEMBERS_PCT_CHANGE_MEAN_30D` float COMMENT 'N_ENROLLED_MEMBERS_PCT_CHANGE_MEAN_30D',
    `N_ENROLLED_MEMBERS_PCT_CHANGE_STD_30D` float COMMENT 'N_ENROLLED_MEMBERS_PCT_CHANGE_STD_30D',
    `N_ELIGIBLE_MEMBERS_PCT_CHANGE_MEAN_30D` float COMMENT 'N_ELIGIBLE_MEMBERS_PCT_CHANGE_MEAN_30D',
    `N_ELIGIBLE_MEMBERS_PCT_CHANGE_STD_30D` float COMMENT 'N_ELIGIBLE_MEMBERS_PCT_CHANGE_STD_30D',
    `MEMBER_ENROLLMENT_RATIO_PCT_CHANGE_MEAN_30D` float COMMENT 'MEMBER_ENROLLMENT_RATIO_PCT_CHANGE_MEAN_30D',
    `MEMBER_ENROLLMENT_RATIO_PCT_CHANGE_STD_30D` float COMMENT 'MEMBER_ENROLLMENT_RATIO_PCT_CHANGE_STD_30D',
    `N_SIGN_UP_MEMBERS` float COMMENT 'N_SIGN_UP_MEMBERS',
    `N_SIGN_UP_MEMBERS_PCT_CHANGE` float COMMENT 'N_SIGN_UP_MEMBERS_PCT_CHANGE',
    `N_SIGN_UP_MEMBERS_MEAN_30D` float COMMENT 'N_SIGN_UP_MEMBERS_MEAN_30D',
    `N_SIGN_UP_MEMBERS_STD_30D` float COMMENT 'N_SIGN_UP_MEMBERS_STD_30D',
    `N_SIGN_UP_MEMBERS_PCT_CHANGE_MEAN_30D` float COMMENT 'N_SIGN_UP_MEMBERS_PCT_CHANGE_MEAN_30D',
    `N_SIGN_UP_MEMBERS_STD_CHANGE_MEAN_30D` float COMMENT 'N_SIGN_UP_MEMBERS_STD_CHANGE_MEAN_30D',
    `SIGN_UP_ELIGIBLE_RATIO` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO',
    `SIGN_UP_ENROLLED_RATIO` float COMMENT 'SIGN_UP_ENROLLED_RATIO',
    `SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE',
    `SIGN_UP_ENROLLED_RATIO_PCT_CHANGE` float COMMENT 'SIGN_UP_ENROLLED_RATIO_PCT_CHANGE',
    `SIGN_UP_ELIGIBLE_RATIO_MEAN_30D` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_MEAN_30D',
    `SIGN_UP_ELIGIBLE_RATIO_STD_30D` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_STD_30D',
    `SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE_MEAN_30D` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE_MEAN_30D',
    `SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE_STD_30D` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE_STD_30D',
    `SIGN_UP_ENROLLED_RATIO_MEAN_30D` float COMMENT 'SIGN_UP_ENROLLED_RATIO_MEAN_30D',
    `SIGN_UP_ENROLLED_RATIO_STD_30D` float COMMENT 'SIGN_UP_ENROLLED_RATIO_STD_30D',
    `SIGN_UP_ENROLLED_RATIO_PCT_CHANGE_MEAN_30D` float COMMENT 'SIGN_UP_ENROLLED_RATIO_PCT_CHANGE_MEAN_30D',
    `SIGN_UP_ENROLLED_RATIO_PCT_CHANGE_STD_30D` float COMMENT 'SIGN_UP_ENROLLED_RATIO_PCT_CHANGE_STD_30D',
    `CHECK_IN_COUNT` float COMMENT 'CHECK_IN_COUNT',
    `DISTINCT_GYMS_VISITED` float COMMENT 'DISTINCT_GYMS_VISITED',
    `MAX_GYM_REPETITION` float COMMENT 'MAX_GYM_REPETITION',
    `SCHEDULED_CLASS_RATIO` float COMMENT 'SCHEDULED_CLASS_RATIO',
    `RETROACTIVE_VALIDATION_RATIO` float COMMENT 'RETROACTIVE_VALIDATION_RATIO',
    `VISIT_TO_VALID_DAYS_RATIO` float COMMENT 'VISIT_TO_VALID_DAYS_RATIO',
    `HISTORICAL_VISITS` integer COMMENT 'HISTORICAL_VISITS',
    `CURRENT_GYM_VISIT_STREAK` integer COMMENT 'CURRENT_GYM_VISIT_STREAK',
    `HISTORICAL_NON_VISITS` integer COMMENT 'HISTORICAL_NON_VISITS',
    `CURRENT_GYM_NON_VISIT_STREAK` integer COMMENT 'CURRENT_GYM_NON_VISIT_STREAK',
    `GYM_VISIT_MAX_STREAK_30D` integer COMMENT 'GYM_VISIT_MAX_STREAK_30D',
    `GYM_NON_VISIT_MAX_STREAK_30D` integer COMMENT 'GYM_NON_VISIT_MAX_STREAK_30D',
    `VISITED_GYM_TO_DISABLE_LAST_30D` float COMMENT 'VISITED_GYM_TO_DISABLE_LAST_30D',
    `RATING_VALUE` float COMMENT 'RATING_VALUE',
    `RATING_VALUE_MIN_LAST_30D` float COMMENT 'RATING_VALUE_MIN_LAST_30D',
    `RATING_VALUE_MEAN_LAST_30D` float COMMENT 'RATING_VALUE_MEAN_LAST_30D',
    `RATING_VALUE_MAX_LAST_30D` float COMMENT 'RATING_VALUE_MAX_LAST_30D',
    `RATING_VALUE_STD_LAST_30D` float COMMENT 'RATING_VALUE_STD_LAST_30D',
    `RATING_VALUE_MIN_HISTORICAL` float COMMENT 'RATING_VALUE_MIN_HISTORICAL',
    `RATING_VALUE_MAX_HISTORICAL` float COMMENT 'RATING_VALUE_MAX_HISTORICAL',
    `GYM_RATING_1_RATIO` float COMMENT 'GYM_RATING_1_RATIO',
    `GYM_RATING_2_RATIO` float COMMENT 'GYM_RATING_2_RATIO',
    `GYM_RATING_3_RATIO` float COMMENT 'GYM_RATING_3_RATIO',
    `GYM_RATING_4_RATIO` float COMMENT 'GYM_RATING_4_RATIO',
    `GYM_RATING_5_RATIO` float COMMENT 'GYM_RATING_5_RATIO',
    `GYM_RATING_COUNT` float COMMENT 'GYM_RATING_COUNT',
    `GYM_RATING_MEAN` float COMMENT 'GYM_RATING_MEAN',
    `GYM_RATING_MINMEAN` float COMMENT 'GYM_RATING_MINMEAN',
    `GYM_RATING_MAXMEAN` float COMMENT 'GYM_RATING_MAXMEAN',
    `GYM_RATING_MEANMEAN` float COMMENT 'GYM_RATING_MEANMEAN',
    `GYM_RATING_STDMEAN` float COMMENT 'GYM_RATING_STDMEAN',
    `APP_USAGE_30D` float COMMENT 'APP_USAGE_30D',
    `WEB_USAGE_RATIO_30D` float COMMENT 'WEB_USAGE_RATIO_30D',
    `APP_SEARCH_30D` integer COMMENT 'APP_SEARCH_30D',
    `APP_CHECK_IN_30D` integer COMMENT 'APP_CHECK_IN_30D',
    `N_TOTAL_BLOCKS` integer COMMENT 'N_TOTAL_BLOCKS',
    `WAS_BLOCKED_SOMETIME` integer COMMENT 'WAS_BLOCKED_SOMETIME',
    `N_BLOCKS_BY_INACTIVITY` integer COMMENT 'N_BLOCKS_BY_INACTIVITY',
    `N_BLOCKS_BY_PASSWORD` integer COMMENT 'N_BLOCKS_BY_PASSWORD',
    `IS_BLOCKED` integer COMMENT 'IS_BLOCKED',
    `DAYS_SINCE_FIRST_BLOCK` integer COMMENT 'DAYS_SINCE_FIRST_BLOCK',
    `DAYS_SINCE_LAST_BLOCK` integer COMMENT 'DAYS_SINCE_LAST_BLOCK',
    `N_DAYS_BLOCKED` integer COMMENT 'N_DAYS_BLOCKED',
    `COMMUNICATION_CHANNEL_others` integer COMMENT 'COMMUNICATION_CHANNEL_others',
    `COMMUNICATION_CHANNEL_chat` integer COMMENT 'COMMUNICATION_CHANNEL_chat',
    `COMMUNICATION_CHANNEL_email` integer COMMENT 'COMMUNICATION_CHANNEL_email',
    `COMMUNICATION_CHANNEL_tel` integer COMMENT 'COMMUNICATION_CHANNEL_tel',
    `COMMUNICATION_CHANNEL_all` integer COMMENT 'COMMUNICATION_CHANNEL_all',
    `COMMUNICATION_CHANNEL_chat_30D` integer COMMENT 'COMMUNICATION_CHANNEL_chat_30D',
    `COMMUNICATION_CHANNEL_email_30D` integer COMMENT 'COMMUNICATION_CHANNEL_email_30D',
    `COMMUNICATION_CHANNEL_tel_30D` integer COMMENT 'COMMUNICATION_CHANNEL_tel_30D',
    `COMMUNICATION_CHANNEL_others_30D` integer COMMENT 'COMMUNICATION_CHANNEL_others_30D',
    `COMMUNICATION_CHANNEL_all_30D` integer COMMENT 'COMMUNICATION_CHANNEL_all_30D',
    `COMMUNICATION_CHANNEL_chat_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_chat_HISTORICAL',
    `COMMUNICATION_CHANNEL_email_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_email_HISTORICAL',
    `COMMUNICATION_CHANNEL_tel_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_tel_HISTORICAL',
    `COMMUNICATION_CHANNEL_others_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_others_HISTORICAL',
    `COMMUNICATION_CHANNEL_all_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_all_HISTORICAL',
    `HAS_SOLVED_TICKET_30D` float COMMENT 'HAS_SOLVED_TICKET_30D',
    `MIN_TIME_TO_SOLVE_30D` float COMMENT 'MIN_TIME_TO_SOLVE_30D',
    `MEAN_TIME_TO_SOLVE_30D` float COMMENT 'MEAN_TIME_TO_SOLVE_30D',
    `MAX_TIME_TO_SOLVE_30D` float COMMENT 'MAX_TIME_TO_SOLVE_30D',
    `HAS_UNSOLVED_TICKET_30D` float COMMENT 'HAS_UNSOLVED_TICKET_30D',
    `HAS_UNSOLVED_TICKET_RATIO_30D` float COMMENT 'HAS_UNSOLVED_TICKET_RATIO_30D',
    `NUM_REPEATED_TICKETS_30D` float COMMENT 'NUM_REPEATED_TICKETS_30D',
    `MOST_FREQ_LAT` float COMMENT 'MOST_FREQ_LAT',
    `MOST_FREQ_LNG` float COMMENT 'MOST_FREQ_LNG',
    `N_CLOSE_GYMS` float COMMENT 'N_CLOSE_GYMS',
    `CLOSE_GYMS_MEAN_CHECKIN_30D` float COMMENT 'CLOSE_GYMS_MEAN_CHECKIN_30D',
    `N_CLOSE_ELIGIBLE_GYMS` float COMMENT 'N_CLOSE_ELIGIBLE_GYMS',
    `CLOSE_ELIGIBLE_GYMS_MEAN_CHECKIN_30D` float COMMENT 'CLOSE_ELIGIBLE_GYMS_MEAN_CHECKIN_30D',
    `DIST_FROM_PREVIOUS_LOCATION` float COMMENT 'DIST_FROM_PREVIOUS_LOCATION',
    `MAIN_GYM_DISTANCE` float COMMENT 'MAIN_GYM_DISTANCE',
    `YEAR` integer COMMENT 'YEAR',
    `MONTH` integer COMMENT 'MONTH',
    `WEEK` integer COMMENT 'WEEK',
    `SCORE_IS_CHURN_VOL_IN_D30` float COMMENT 'SCORE_IS_CHURN_VOL_IN_D30',
    `RANK_SCORE_IS_CHURN_VOL_IN_D30` float COMMENT 'RANK_SCORE_IS_CHURN_VOL_IN_D30',
    `PERCENTILE_SCORE_IS_CHURN_VOL_IN_D30` float COMMENT 'PERCENTILE_SCORE_IS_CHURN_VOL_IN_D30',
    `CLUSTER` integer COMMENT 'CLUSTER',
    `REGION` string COMMENT 'REGION',
    `USER_MARGIN` float COMMENT 'USER_MARGIN'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/churn_mt_scored_clustered_valued/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.churn_mt_scored_valued` (
    `user_id` integer COMMENT 'user_id',
    `TS_DAY` date COMMENT 'TS_DAY',
    `IS_CHURN_IN_D30` integer COMMENT 'IS_CHURN_IN_D30',
    `IS_DISMISSED_D30_BEFORE_TO_D30_AFTER` integer COMMENT 'IS_DISMISSED_D30_BEFORE_TO_D30_AFTER',
    `IS_CHURN_VOL_IN_D30` integer COMMENT 'IS_CHURN_VOL_IN_D30',
    `country_id` float COMMENT 'country_id',
    `DAYS_SINCE_FIRST_CREATION` float COMMENT 'DAYS_SINCE_FIRST_CREATION',
    `DAYS_SINCE_FIRST_CONFIRMATION` float COMMENT 'DAYS_SINCE_FIRST_CONFIRMATION',
    `DAYS_SINCE_FIRST_SIGNIN` float COMMENT 'DAYS_SINCE_FIRST_SIGNIN',
    `DAYS_SINCE_FIRST_PAYMENT` float COMMENT 'DAYS_SINCE_FIRST_PAYMENT',
    `DAYS_SINCE_FIRST_GYM_VISIT` float COMMENT 'DAYS_SINCE_FIRST_GYM_VISIT',
    `DAYS_FROM_CREATION_TO_SIGNIN` float COMMENT 'DAYS_FROM_CREATION_TO_SIGNIN',
    `DAYS_FROM_CREATION_TO_PAYMENT` float COMMENT 'DAYS_FROM_CREATION_TO_PAYMENT',
    `DAYS_FROM_SIGNIN_TO_PAYMENT` float COMMENT 'DAYS_FROM_SIGNIN_TO_PAYMENT',
    `DAYS_FROM_PAYMENT_TO_GYM_VISIT` float COMMENT 'DAYS_FROM_PAYMENT_TO_GYM_VISIT',
    `GAP_SIZE_SUM_HISTORICAL` integer COMMENT 'GAP_SIZE_SUM_HISTORICAL',
    `GAP_SIZE_MAX_HISTORICAL` integer COMMENT 'GAP_SIZE_MAX_HISTORICAL',
    `GAP_COUNT_SUM_HISTORICAL` integer COMMENT 'GAP_COUNT_SUM_HISTORICAL',
    `GAP_COUNT_SUM_30D` integer COMMENT 'GAP_COUNT_SUM_30D',
    `NUM_FAMILY_MEMBERS` float COMMENT 'NUM_FAMILY_MEMBERS',
    `IS_ACCOUNT_HOLDER` float COMMENT 'IS_ACCOUNT_HOLDER',
    `user_company_id` integer COMMENT 'user_company_id',
    `user_parent_company_id` integer COMMENT 'user_parent_company_id',
    `PAID_PRICE` float COMMENT 'PAID_PRICE',
    `PLAN_PRICE` float COMMENT 'PLAN_PRICE',
    `CURRENT_PAYMENT_SEQUENCE_COUNT` integer COMMENT 'CURRENT_PAYMENT_SEQUENCE_COUNT',
    `PAID_TO_PLAN_PRICE_RATIO` float COMMENT 'PAID_TO_PLAN_PRICE_RATIO',
    `PAYMENT_METHOD_AdyenIdealPayment` integer COMMENT 'PAYMENT_METHOD_AdyenIdealPayment',
    `PAYMENT_METHOD_RemotePersonCard` integer COMMENT 'PAYMENT_METHOD_RemotePersonCard',
    `PAYMENT_METHOD_Payroll` integer COMMENT 'PAYMENT_METHOD_Payroll',
    `PAYMENT_METHOD_BankAccount` integer COMMENT 'PAYMENT_METHOD_BankAccount',
    `PAYMENT_METHOD_PaypalAgreement` integer COMMENT 'PAYMENT_METHOD_PaypalAgreement',
    `PURCHASE_TYPE_Primeira_compra` integer COMMENT 'PURCHASE_TYPE_Primeira_compra',
    `PURCHASE_TYPE_Renovacao_de_plano` integer COMMENT 'PURCHASE_TYPE_Renovacao_de_plano',
    `COMPANY_TYPE_Employees` integer COMMENT 'COMPANY_TYPE_Employees',
    `COMPANY_TYPE_Former_Members` integer COMMENT 'COMPANY_TYPE_Former_Members',
    `COMPANY_TYPE_Family_Member` integer COMMENT 'COMPANY_TYPE_Family_Member',
    `ACQUISITION_CHANNEL_B2B` integer COMMENT 'ACQUISITION_CHANNEL_B2B',
    `ACQUISITION_CHANNEL_B2C` integer COMMENT 'ACQUISITION_CHANNEL_B2C',
    `IS_D2_PAYMENT_FAILURE` integer COMMENT 'IS_D2_PAYMENT_FAILURE',
    `N_OF_PAYMENT_FAILURE_LAST_30D` integer COMMENT 'N_OF_PAYMENT_FAILURE_LAST_30D',
    `N_OF_IS_D2_PAYMENT_FAILURE_LAST_30D` integer COMMENT 'N_OF_IS_D2_PAYMENT_FAILURE_LAST_30D',
    `TOTAL_REFUND` float COMMENT 'TOTAL_REFUND',
    `DELTA_DAYS_REFUND_TO_VALID_END` float COMMENT 'DELTA_DAYS_REFUND_TO_VALID_END',
    `RATIO_REFUND_TO_PAID` float COMMENT 'RATIO_REFUND_TO_PAID',
    `REFUNDED_SAME_DAY_OR_NEXT` integer COMMENT 'REFUNDED_SAME_DAY_OR_NEXT',
    `D30_BEFORE_TO_CURRENT_PAID_PRICE_RATIO` float COMMENT 'D30_BEFORE_TO_CURRENT_PAID_PRICE_RATIO',
    `D30_BEFORE_TO_CURRENT_PLAN_PRICE_RATIO` float COMMENT 'D30_BEFORE_TO_CURRENT_PLAN_PRICE_RATIO',
    `D30_BEFORE_TO_CURRENT_PAID_TO_PLAN_PRICE_RATIO` float COMMENT 'D30_BEFORE_TO_CURRENT_PAID_TO_PLAN_PRICE_RATIO',
    `IS_BILL_SHOCK_30D` float COMMENT 'IS_BILL_SHOCK_30D',
    `BILL_SHOCK_INCREASE_RATIO_30D` float COMMENT 'BILL_SHOCK_INCREASE_RATIO_30D',
    `CARD_EXPIRATION_NEXT_30D` float COMMENT 'CARD_EXPIRATION_NEXT_30D',
    `CARD_EXPIRATION_NEXT_45D` float COMMENT 'CARD_EXPIRATION_NEXT_45D',
    `CARD_EXPIRATION_NEXT_60D` float COMMENT 'CARD_EXPIRATION_NEXT_60D',
    `N_ELIGIBLE_MEMBERS` integer COMMENT 'N_ELIGIBLE_MEMBERS',
    `N_ENROLLED_MEMBERS` integer COMMENT 'N_ENROLLED_MEMBERS',
    `MEMBER_ENROLLMENT_RATIO` float COMMENT 'MEMBER_ENROLLMENT_RATIO',
    `N_ENROLLED_MEMBERS_PCT_CHANGE` float COMMENT 'N_ENROLLED_MEMBERS_PCT_CHANGE',
    `N_ELIGIBLE_MEMBERS_PCT_CHANGE` float COMMENT 'N_ELIGIBLE_MEMBERS_PCT_CHANGE',
    `MEMBER_ENROLLMENT_RATIO_PCT_CHANGE` float COMMENT 'MEMBER_ENROLLMENT_RATIO_PCT_CHANGE',
    `N_ENROLLED_MEMBERS_PCT_CHANGE_MEAN_30D` float COMMENT 'N_ENROLLED_MEMBERS_PCT_CHANGE_MEAN_30D',
    `N_ENROLLED_MEMBERS_PCT_CHANGE_STD_30D` float COMMENT 'N_ENROLLED_MEMBERS_PCT_CHANGE_STD_30D',
    `N_ELIGIBLE_MEMBERS_PCT_CHANGE_MEAN_30D` float COMMENT 'N_ELIGIBLE_MEMBERS_PCT_CHANGE_MEAN_30D',
    `N_ELIGIBLE_MEMBERS_PCT_CHANGE_STD_30D` float COMMENT 'N_ELIGIBLE_MEMBERS_PCT_CHANGE_STD_30D',
    `MEMBER_ENROLLMENT_RATIO_PCT_CHANGE_MEAN_30D` float COMMENT 'MEMBER_ENROLLMENT_RATIO_PCT_CHANGE_MEAN_30D',
    `MEMBER_ENROLLMENT_RATIO_PCT_CHANGE_STD_30D` float COMMENT 'MEMBER_ENROLLMENT_RATIO_PCT_CHANGE_STD_30D',
    `N_SIGN_UP_MEMBERS` float COMMENT 'N_SIGN_UP_MEMBERS',
    `N_SIGN_UP_MEMBERS_PCT_CHANGE` float COMMENT 'N_SIGN_UP_MEMBERS_PCT_CHANGE',
    `N_SIGN_UP_MEMBERS_MEAN_30D` float COMMENT 'N_SIGN_UP_MEMBERS_MEAN_30D',
    `N_SIGN_UP_MEMBERS_STD_30D` float COMMENT 'N_SIGN_UP_MEMBERS_STD_30D',
    `N_SIGN_UP_MEMBERS_PCT_CHANGE_MEAN_30D` float COMMENT 'N_SIGN_UP_MEMBERS_PCT_CHANGE_MEAN_30D',
    `N_SIGN_UP_MEMBERS_STD_CHANGE_MEAN_30D` float COMMENT 'N_SIGN_UP_MEMBERS_STD_CHANGE_MEAN_30D',
    `SIGN_UP_ELIGIBLE_RATIO` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO',
    `SIGN_UP_ENROLLED_RATIO` float COMMENT 'SIGN_UP_ENROLLED_RATIO',
    `SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE',
    `SIGN_UP_ENROLLED_RATIO_PCT_CHANGE` float COMMENT 'SIGN_UP_ENROLLED_RATIO_PCT_CHANGE',
    `SIGN_UP_ELIGIBLE_RATIO_MEAN_30D` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_MEAN_30D',
    `SIGN_UP_ELIGIBLE_RATIO_STD_30D` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_STD_30D',
    `SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE_MEAN_30D` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE_MEAN_30D',
    `SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE_STD_30D` float COMMENT 'SIGN_UP_ELIGIBLE_RATIO_PCT_CHANGE_STD_30D',
    `SIGN_UP_ENROLLED_RATIO_MEAN_30D` float COMMENT 'SIGN_UP_ENROLLED_RATIO_MEAN_30D',
    `SIGN_UP_ENROLLED_RATIO_STD_30D` float COMMENT 'SIGN_UP_ENROLLED_RATIO_STD_30D',
    `SIGN_UP_ENROLLED_RATIO_PCT_CHANGE_MEAN_30D` float COMMENT 'SIGN_UP_ENROLLED_RATIO_PCT_CHANGE_MEAN_30D',
    `SIGN_UP_ENROLLED_RATIO_PCT_CHANGE_STD_30D` float COMMENT 'SIGN_UP_ENROLLED_RATIO_PCT_CHANGE_STD_30D',
    `CHECK_IN_COUNT` float COMMENT 'CHECK_IN_COUNT',
    `DISTINCT_GYMS_VISITED` float COMMENT 'DISTINCT_GYMS_VISITED',
    `MAX_GYM_REPETITION` float COMMENT 'MAX_GYM_REPETITION',
    `SCHEDULED_CLASS_RATIO` float COMMENT 'SCHEDULED_CLASS_RATIO',
    `RETROACTIVE_VALIDATION_RATIO` float COMMENT 'RETROACTIVE_VALIDATION_RATIO',
    `VISIT_TO_VALID_DAYS_RATIO` float COMMENT 'VISIT_TO_VALID_DAYS_RATIO',
    `HISTORICAL_VISITS` integer COMMENT 'HISTORICAL_VISITS',
    `CURRENT_GYM_VISIT_STREAK` integer COMMENT 'CURRENT_GYM_VISIT_STREAK',
    `HISTORICAL_NON_VISITS` integer COMMENT 'HISTORICAL_NON_VISITS',
    `CURRENT_GYM_NON_VISIT_STREAK` integer COMMENT 'CURRENT_GYM_NON_VISIT_STREAK',
    `GYM_VISIT_MAX_STREAK_30D` integer COMMENT 'GYM_VISIT_MAX_STREAK_30D',
    `GYM_NON_VISIT_MAX_STREAK_30D` integer COMMENT 'GYM_NON_VISIT_MAX_STREAK_30D',
    `VISITED_GYM_TO_DISABLE_LAST_30D` float COMMENT 'VISITED_GYM_TO_DISABLE_LAST_30D',
    `RATING_VALUE` float COMMENT 'RATING_VALUE',
    `RATING_VALUE_MIN_LAST_30D` float COMMENT 'RATING_VALUE_MIN_LAST_30D',
    `RATING_VALUE_MEAN_LAST_30D` float COMMENT 'RATING_VALUE_MEAN_LAST_30D',
    `RATING_VALUE_MAX_LAST_30D` float COMMENT 'RATING_VALUE_MAX_LAST_30D',
    `RATING_VALUE_STD_LAST_30D` float COMMENT 'RATING_VALUE_STD_LAST_30D',
    `RATING_VALUE_MIN_HISTORICAL` float COMMENT 'RATING_VALUE_MIN_HISTORICAL',
    `RATING_VALUE_MAX_HISTORICAL` float COMMENT 'RATING_VALUE_MAX_HISTORICAL',
    `GYM_RATING_1_RATIO` float COMMENT 'GYM_RATING_1_RATIO',
    `GYM_RATING_2_RATIO` float COMMENT 'GYM_RATING_2_RATIO',
    `GYM_RATING_3_RATIO` float COMMENT 'GYM_RATING_3_RATIO',
    `GYM_RATING_4_RATIO` float COMMENT 'GYM_RATING_4_RATIO',
    `GYM_RATING_5_RATIO` float COMMENT 'GYM_RATING_5_RATIO',
    `GYM_RATING_COUNT` float COMMENT 'GYM_RATING_COUNT',
    `GYM_RATING_MEAN` float COMMENT 'GYM_RATING_MEAN',
    `GYM_RATING_MINMEAN` float COMMENT 'GYM_RATING_MINMEAN',
    `GYM_RATING_MAXMEAN` float COMMENT 'GYM_RATING_MAXMEAN',
    `GYM_RATING_MEANMEAN` float COMMENT 'GYM_RATING_MEANMEAN',
    `GYM_RATING_STDMEAN` float COMMENT 'GYM_RATING_STDMEAN',
    `APP_USAGE_30D` float COMMENT 'APP_USAGE_30D',
    `WEB_USAGE_RATIO_30D` float COMMENT 'WEB_USAGE_RATIO_30D',
    `APP_SEARCH_30D` integer COMMENT 'APP_SEARCH_30D',
    `APP_CHECK_IN_30D` integer COMMENT 'APP_CHECK_IN_30D',
    `N_TOTAL_BLOCKS` integer COMMENT 'N_TOTAL_BLOCKS',
    `WAS_BLOCKED_SOMETIME` integer COMMENT 'WAS_BLOCKED_SOMETIME',
    `N_BLOCKS_BY_INACTIVITY` integer COMMENT 'N_BLOCKS_BY_INACTIVITY',
    `N_BLOCKS_BY_PASSWORD` integer COMMENT 'N_BLOCKS_BY_PASSWORD',
    `IS_BLOCKED` integer COMMENT 'IS_BLOCKED',
    `DAYS_SINCE_FIRST_BLOCK` integer COMMENT 'DAYS_SINCE_FIRST_BLOCK',
    `DAYS_SINCE_LAST_BLOCK` integer COMMENT 'DAYS_SINCE_LAST_BLOCK',
    `N_DAYS_BLOCKED` integer COMMENT 'N_DAYS_BLOCKED',
    `COMMUNICATION_CHANNEL_others` integer COMMENT 'COMMUNICATION_CHANNEL_others',
    `COMMUNICATION_CHANNEL_chat` integer COMMENT 'COMMUNICATION_CHANNEL_chat',
    `COMMUNICATION_CHANNEL_email` integer COMMENT 'COMMUNICATION_CHANNEL_email',
    `COMMUNICATION_CHANNEL_tel` integer COMMENT 'COMMUNICATION_CHANNEL_tel',
    `COMMUNICATION_CHANNEL_all` integer COMMENT 'COMMUNICATION_CHANNEL_all',
    `COMMUNICATION_CHANNEL_chat_30D` integer COMMENT 'COMMUNICATION_CHANNEL_chat_30D',
    `COMMUNICATION_CHANNEL_email_30D` integer COMMENT 'COMMUNICATION_CHANNEL_email_30D',
    `COMMUNICATION_CHANNEL_tel_30D` integer COMMENT 'COMMUNICATION_CHANNEL_tel_30D',
    `COMMUNICATION_CHANNEL_others_30D` integer COMMENT 'COMMUNICATION_CHANNEL_others_30D',
    `COMMUNICATION_CHANNEL_all_30D` integer COMMENT 'COMMUNICATION_CHANNEL_all_30D',
    `COMMUNICATION_CHANNEL_chat_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_chat_HISTORICAL',
    `COMMUNICATION_CHANNEL_email_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_email_HISTORICAL',
    `COMMUNICATION_CHANNEL_tel_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_tel_HISTORICAL',
    `COMMUNICATION_CHANNEL_others_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_others_HISTORICAL',
    `COMMUNICATION_CHANNEL_all_HISTORICAL` integer COMMENT 'COMMUNICATION_CHANNEL_all_HISTORICAL',
    `HAS_SOLVED_TICKET_30D` float COMMENT 'HAS_SOLVED_TICKET_30D',
    `MIN_TIME_TO_SOLVE_30D` float COMMENT 'MIN_TIME_TO_SOLVE_30D',
    `MEAN_TIME_TO_SOLVE_30D` float COMMENT 'MEAN_TIME_TO_SOLVE_30D',
    `MAX_TIME_TO_SOLVE_30D` float COMMENT 'MAX_TIME_TO_SOLVE_30D',
    `HAS_UNSOLVED_TICKET_30D` float COMMENT 'HAS_UNSOLVED_TICKET_30D',
    `HAS_UNSOLVED_TICKET_RATIO_30D` float COMMENT 'HAS_UNSOLVED_TICKET_RATIO_30D',
    `NUM_REPEATED_TICKETS_30D` float COMMENT 'NUM_REPEATED_TICKETS_30D',
    `MOST_FREQ_LAT` float COMMENT 'MOST_FREQ_LAT',
    `MOST_FREQ_LNG` float COMMENT 'MOST_FREQ_LNG',
    `N_CLOSE_GYMS` float COMMENT 'N_CLOSE_GYMS',
    `CLOSE_GYMS_MEAN_CHECKIN_30D` float COMMENT 'CLOSE_GYMS_MEAN_CHECKIN_30D',
    `N_CLOSE_ELIGIBLE_GYMS` float COMMENT 'N_CLOSE_ELIGIBLE_GYMS',
    `CLOSE_ELIGIBLE_GYMS_MEAN_CHECKIN_30D` float COMMENT 'CLOSE_ELIGIBLE_GYMS_MEAN_CHECKIN_30D',
    `DIST_FROM_PREVIOUS_LOCATION` float COMMENT 'DIST_FROM_PREVIOUS_LOCATION',
    `MAIN_GYM_DISTANCE` float COMMENT 'MAIN_GYM_DISTANCE',
    `YEAR` integer COMMENT 'YEAR',
    `MONTH` integer COMMENT 'MONTH',
    `WEEK` integer COMMENT 'WEEK',
    `SCORE_IS_CHURN_VOL_IN_D30` float COMMENT 'SCORE_IS_CHURN_VOL_IN_D30',
    `RANK_SCORE_IS_CHURN_VOL_IN_D30` float COMMENT 'RANK_SCORE_IS_CHURN_VOL_IN_D30',
    `PERCENTILE_SCORE_IS_CHURN_VOL_IN_D30` float COMMENT 'PERCENTILE_SCORE_IS_CHURN_VOL_IN_D30',
    `REGION` string COMMENT 'REGION',
    `USER_MARGIN` float COMMENT 'USER_MARGIN'
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/churn_mt_scored_valued/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gw_usage` (
  `partner_name` string COMMENT 'partner_name',
  `email` string COMMENT 'user_email',
  `event_type` string COMMENT 'event_type',
  `timestamp` timestamp COMMENT 'timestamp',
  `gpw_id` string COMMENT 'gpw_id',
  `event_duration` integer COMMENT 'event_duration',
  `viewing_duration` integer COMMENT 'viewing_duration',
  `event_subcategory` string COMMENT 'event_subcategory',
  `event_title` string COMMENT 'event_title',
  `event_subtitle` string COMMENT 'event_subtitle',
  `event_equipment` string COMMENT 'event_equipment',
  `timezone` string COMMENT 'timezone',
  `geo_latitude` float COMMENT 'geo_latitude',
  `geo_longitude` float COMMENT 'geo_longitude',
  `device` string COMMENT 'device',
  `ip` string COMMENT 'ip'
  )
  COMMENT ''
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gw_usage/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gym_geolocation` (
  `gym_id` integer COMMENT 'gym_id',
  `gym_name` string COMMENT 'gym_name',
  `gym_network_id` integer COMMENT 'gym_network_id',
  `gym_network_name` string COMMENT 'gym_network_name',
  `latitude` string COMMENT 'latitude',
  `longitude` string COMMENT 'longitude',
  `tz_name` string COMMENT 'tz_name',
  `utc_daylights_offset` integer COMMENT 'utc_daylights_offset',
  `utc_offset` integer COMMENT 'utc_offset'
  )
  COMMENT ''
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gym_geolocation/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gym_guarantee` (
  `key` string COMMENT 'contract_indentifier',
  `country` string COMMENT 'country',
  `gym_name` string COMMENT 'gym_name',
  `qt_guarantee_contracts` integer COMMENT 'in_case_multiple_guarantee_identify_contract',
  `guarantee_start_month` timestamp COMMENT 'guarantee_start_month',
  `contract_length` integer COMMENT 'guarantee_contract',
  `total_commitment` decimal(12, 2) COMMENT 'total_contract_amount',
  `currency` string COMMENT 'currency',
  `total_guarantee_amount` decimal(12, 2) COMMENT 'cases_of_ADVANCE_there_is_no_value_here',
  `guarantee_expiration` string COMMENT 'period_that_triggers_the_payment',
  `guarantee_flag` string COMMENT 'Boolean',
  `advance_flag` string COMMENT 'Boolean',
  `open_ended_flag` string COMMENT 'flag_to_see_if_there_is_a_end_period',
  `first_year_amount` decimal(12, 2) COMMENT 'first_year_amount',
  `second_year_amount` decimal(12, 2) COMMENT 'second_year_amount',
  `third_year_amount` decimal(12, 2) COMMENT 'third_year_amount',
  `fourth_year_amount` decimal(12, 2) COMMENT 'fourth_year_amount',
  `fifth_year_amount` decimal(12, 2) COMMENT 'fifth_year_amount',
  `cash_transferred_to_date` decimal(12, 2) COMMENT 'cash_we_paid_til_now',
  `payout_consumption` decimal(12, 2) COMMENT 'percentage_of_total_gym_expense_applied_to_advance_or_guarantee',
  `if_fixed_amount` decimal(12, 2) COMMENT 'amount_to_be_deducted_from_advance_or_guarantee_regardless_visits',
  `start_consumption_month` timestamp COMMENT 'start_consumption_month',
  `future_cash_transfers` decimal(12, 2) COMMENT 'Future_contract_clauses_tranches_rule',
  `first_tranch_amount` decimal(12, 2) COMMENT 'Future_contract_clauses_tranches_rule',
  `first_tranch_trigger` string COMMENT 'Future_contract_clauses_tranches_rule',
  `first_tranch_date` timestamp COMMENT 'Future_contract_clauses_tranches_rule',
  `second_tranch_amount` decimal(12, 2) COMMENT 'Future_contract_clauses_tranches_rule',
  `second_tranch_trigger` string COMMENT 'Future_contract_clauses_tranches_rule',
  `second_tranch_date` timestamp COMMENT 'Future_contract_clauses_tranches_rule',
  `third_tranch_amount` decimal(12, 2) COMMENT 'Future_contract_clauses_tranches_rule',
  `third_tranch_trigger` string COMMENT 'Future_contract_clauses_tranches_rule',
  `third_tranch_date` timestamp COMMENT 'Future_contract_clauses_tranches_rule'
  )
  COMMENT ''
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gym_guarantee/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gym_guarantee_ids` (
  `Key` string COMMENT 'Key',
  `id_type` string COMMENT 'id_type',
  `id` integer COMMENT 'id'
  )
  COMMENT ''
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gym_guarantee_ids/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gym_wishlist` (
  `gym_id` integer COMMENT 'gym_id',
  `gym_network_id` integer COMMENT 'gym_network_id'
  )
  COMMENT ''
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gym_wishlist/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.companies_digital_launch_outSF`(
`company_id` string COMMENT 'company_id',
`digital_launch_date` string COMMENT 'digital_launch_date',
`rn` string COMMENT 'rn'
)
COMMENT ''
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 STORED AS TEXTFILE
 LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/companies_digital_launch_outSF/'
 TBLPROPERTIES (
     'classification'='csv',
     'skip.header.line.count'='1',
     'typeOfData'='file'
);


CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.bigdatacorp_users_enriched_emails`(
`person_id` string COMMENT 'person_id',
`cpf` string COMMENT 'cpf',
`email` string COMMENT 'email',
`domain` string COMMENT 'domain',
`email_user` string COMMENT 'email_user',
`type` string COMMENT 'type',
`passages_with_entity` string COMMENT 'passages_with_entity',
`suspect_passages_with_entity` string COMMENT 'suspect_passages_with_entity',
`crawling_passages_with_entity` string COMMENT 'crawling_passages_with_entity',
`validation_passages_with_entity` string COMMENT 'validation_passages_with_entity',
`consult_passages_with_entity` string COMMENT 'consult_passages_with_entity',
`monthly_avg_of_passages_with_entity` string COMMENT 'monthly_avg_of_passages_with_entity',
`passages_total` string COMMENT 'passages_total',
`suspect_passages_total` string COMMENT 'suspect_passages_total',
`crawling_passages_total` string COMMENT 'crawling_passages_total',
`validation_passages_total` string COMMENT 'validation_passages_total',
`consult_passages_total` string COMMENT 'consult_passages_total',
`monthly_avg_of_total_passages` string COMMENT 'monthly_avg_of_total_passages',
`number_of_associated_entities` string COMMENT 'number_of_associated_entities',
`priority_for_entity` string COMMENT 'priority_for_entity',
`main_email_for_entity` boolean COMMENT 'main_email_for_entity',
`most_recent_email_for_entity` boolean COMMENT 'most_recent_email_for_entity',
`main_email_for_other_entity` boolean COMMENT 'main_email_for_other_entity',
`most_recent_email_for_other_entity` boolean COMMENT 'most_recent_email_for_other_entity',
`active` boolean COMMENT 'active',
`validation_status` string COMMENT 'validation_status',
`last_validation_date` date COMMENT 'last_validation_date',
`first_passage_date_with_entity` date COMMENT 'first_passage_date_with_entity',
`last_passage_date_with_entity` date COMMENT 'last_passage_date_with_entity',
`first_passage_date` date COMMENT 'first_passage_date',
`last_passage_date` date COMMENT 'last_passage_date',
`creation_date` date COMMENT 'creation_date',
`capture_date` date COMMENT 'capture_date'
)
COMMENT ''
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 STORED AS TEXTFILE
 LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/bigdatacorp_users_enriched_emails/'
 TBLPROPERTIES (
     'classification'='csv',
     'skip.header.line.count'='1',
     'typeOfData'='file'
);


CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.bigdatacorp_users_enriched_telephones`(
`person_id` string COMMENT 'person_id',
`cpf` string COMMENT 'cpf',
`international_code` string COMMENT 'international_code',
`local_code` string COMMENT 'local_code',
`telephone_number` string COMMENT 'telephone_number',
`complement` string COMMENT 'complement',
`type` string COMMENT 'type',
`passages_with_entity` string COMMENT 'passages_with_entity',
`suspect_passages_with_entity` string COMMENT 'suspect_passages_with_entity',
`crawling_passages_with_entity` string COMMENT 'crawling_passages_with_entity',
`validation_passages_with_entity` string COMMENT 'validation_passages_with_entity',
`consult_passages_with_entity` string COMMENT 'consult_passages_with_entity',
`monthly_avg_of_passages_with_entity` string COMMENT 'monthly_avg_of_passages_with_entity',
`passages_total` string COMMENT 'passages_total',
`suspect_passages_total` string COMMENT 'suspect_passages_total',
`crawling_passages_total` string COMMENT 'crawling_passages_total',
`validation_passages_total` string COMMENT 'validation_passages_total',
`consult_passages_total` string COMMENT 'consult_passages_total',
`monthly_avg_of_total_passages` string COMMENT 'monthly_avg_of_total_passages',
`number_of_associated_entities` string COMMENT 'number_of_associated_entities',
`priority_for_entity` string COMMENT 'priority_for_entity',
`main_telephone_for_entity` boolean COMMENT 'main_telephone_for_entity',
`most_recent_telephone_for_entity` boolean COMMENT 'most_recent_telephone_for_entity',
`main_telephone_for_other_entity` boolean COMMENT 'main_telephone_for_other_entity',
`most_recent_telephone_for_other_entity` boolean COMMENT 'most_recent_telephone_for_other_entity',
`active` boolean COMMENT 'active',
`do_not_disturb_list` boolean COMMENT 'do_not_disturb_list',
`telecom` string COMMENT 'telecom',
`plan_type` string COMMENT 'plan_type',
`portability_historic` string COMMENT 'portability_historic',
`validation_status` string COMMENT 'validation_status',
`last_validation_date` date COMMENT 'last_validation_date',
`first_passage_date_with_entity` date COMMENT 'first_passage_date_with_entity',
`last_passage_date_with_entity` date COMMENT 'last_passage_date_with_entity',
`first_passage_date` date COMMENT 'first_passage_date',
`last_passage_date` date COMMENT 'last_passage_date',
`creation_date` date COMMENT 'creation_date',
`capture_date` date COMMENT 'capture_date'
)
COMMENT ''
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 STORED AS TEXTFILE
 LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/bigdatacorp_users_enriched_telephones/'
 TBLPROPERTIES (
     'classification'='csv',
     'skip.header.line.count'='1',
     'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.bigdatacorp_users_enriched_data`(
`person_id` string COMMENT 'person_id',
`cpf` string COMMENT 'cpf',
`cpf1` string COMMENT 'cpf1',
`origin_country` string COMMENT 'origin_country',
`orgao_emissor` string COMMENT 'orgao_emissor',
`document_status` string COMMENT 'document_status',
`rg` string COMMENT 'rg',
`cnh` string COMMENT 'cnh',
`voter_registration` string COMMENT 'voter_registration',
`pis` string COMMENT 'pis',
`name` string COMMENT 'name',
`common_name` string COMMENT 'common_name',
`padronized_name` string COMMENT 'padronized_name',
`social_name` string COMMENT 'social_name',
`name_oneness` integer COMMENT 'nome_oneness',
`first_name_oneness` integer COMMENT 'first_name_oneness',
`first_and_last_name_oneness` integer COMMENT 'first_and_last_name_oneness',
`mothers_name` string COMMENT 'mothers_name',
`fathers_name` string COMMENT 'fathers_name',
`birthday` date COMMENT 'birthday',
`age` integer COMMENT 'age',
`gender` string COMMENT 'gender',
`zodiac_sign` string COMMENT 'zodiac_sign',
`chinese_zodiac_sign` string COMMENT 'chinese_zodiac_sign',
`death_indication` boolean COMMENT 'death_indication',
`death_indication_origin` string COMMENT 'death_indication_origin',
`death_indication_year` integer COMMENT 'death_indication_year',
`first_capture_date` date COMMENT 'first_capture_date',
`last_capture_date` date COMMENT 'last_capture_date',
`match_rate_with_entry_input` date COMMENT 'match_rate_with_entry_input'
)
COMMENT ''
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 STORED AS TEXTFILE
 LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/bigdatacorp_users_enriched_data/'
 TBLPROPERTIES (
     'classification'='csv',
     'skip.header.line.count'='1',
     'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.bigdatacorp_validated_emails`(
`email` string COMMENT 'email',
`email_of_entry` string COMMENT 'email_of_entry',
`email_normalized` string COMMENT 'email_normalized',
`status` string COMMENT 'status',
`adress` string COMMENT 'adress',
`account` string COMMENT 'account',
`email_domain` string COMMENT 'email_domain',
`temporary_email` boolean COMMENT 'temporary_email',
`group_email` boolean COMMENT 'group_email',
`risky_email` boolean COMMENT 'risky_email',
`risky_domain` boolean COMMENT 'risky_domain',
`email_of_junkmail` boolean COMMENT 'email_of_junkmail',
`email_with_restriction` boolean COMMENT 'email_with_restriction',
`email_acceptall` boolean COMMENT 'email_acceptall',
`possible_spam_trap` boolean COMMENT 'possible_spam_trap',
`valid_syntax` boolean COMMENT 'valid_syntax',
`normalized_syntax` boolean COMMENT 'normalized_syntax'
)
COMMENT ''
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 STORED AS TEXTFILE
 LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/bigdatacorp_validated_emails/'
 TBLPROPERTIES (
     'classification'='csv',
     'skip.header.line.count'='1',
     'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.partnerops_bt_canibalizados`(
`documento_cliente` string COMMENT 'Bodytech`s client national number',
`id_unidade` integer COMMENT 'unique identifier for BT locations',
`nome_unidade` string COMMENT 'Bt location name',
`dt_fim_plano` timestamp COMMENT 'expiration plan date',
`dt_inativacao` timestamp COMMENT 'cancelation date'
)
COMMENT ''
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 STORED AS TEXTFILE
 LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/partnerops_bt_canibalizados/'
 TBLPROPERTIES (
     'classification'='csv',
     'skip.header.line.count'='1',
     'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.partnerops_bt_locations`(
`id_unidade` integer COMMENT 'unique identifier for BT locations',
`nome_unidade` string COMMENT 'Bt location name',
`gym_id` integer COMMENT 'Gympass gym ID unique identifier'
)
COMMENT ''
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 STORED AS TEXTFILE
 LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/partnerops_bt_locations/'
 TBLPROPERTIES (
     'classification'='csv',
     'skip.header.line.count'='1',
     'typeOfData'='file'
);


CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.user_behavior_profile` (
    `person_id` integer COMMENT '',
    `company_id` integer COMMENT '',
    `country_title` string COMMENT '',
    `visits_total` integer COMMENT '',
    `visits_in_person` integer COMMENT '',
    `visits_live_class` integer COMMENT '',
    `visits_personal_trainer` integer COMMENT '',
    `distinct_gyms_total` integer COMMENT '',
      `most_frequent_gym` integer COMMENT '',
    `w_checkins` integer COMMENT '',
    `distinct_w_apps` integer COMMENT '',
    `most_frequent_w_app` string COMMENT '',
    `count_app_sessions` integer COMMENT '',
    `first_visit` integer COMMENT ''
)
COMMENT ''
PARTITIONED BY (
  `first_day_of_week` int COMMENT ''
)
STORED AS PARQUET
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gympass/user_behavior_profile/'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='Snappy',
    'typeOfData'='file');

CREATE EXTERNAL TABLE `analytics.wellness_usage_event_ignored`(
  `_app_id` string COMMENT 'The repository name of the system responsible for triggering this event',
  `_event_type` string COMMENT 'Name of the event, should be equal to the name of the table',
  `_event_id` string COMMENT 'Unique identifier of event. Must be UUID v4',
  `_event_time` bigint COMMENT 'Time the event was generated, formatted as unix epoch time in milliseconds UTC+0',
  `refer_event_id` string COMMENT 'The id of event to be ignored',
  `reason` string COMMENT 'The reason as to why the event was ignored'
  )
  COMMENT 'This event is received from gympass wellness'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gympass_wellness/wellness_usage_event_ignored/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file');

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.product_metrics` (
    `user_key` string COMMENT '',
    `metric_fact` string COMMENT '',
    `metric_slice` string COMMENT '',
    `current_plan_value_local` decimal(12, 2) COMMENT '',
    `last_plan_value_local` decimal(12, 2) COMMENT '',
    `gym_id` integer COMMENT '',
    `client_id` bigint COMMENT '',
    `person_id` bigint COMMENT '',
    `country_id` integer COMMENT ''
)
COMMENT ''
PARTITIONED BY (
  `dt` int COMMENT '',
  `metric` string COMMENT ''
)
STORED AS PARQUET
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gympass/product_metrics/'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='Snappy',
    'typeOfData'='file');

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.us_addreasseable_market` (
    `company_name` string COMMENT 'company_name',
    `sic_code` string COMMENT 'sic_code',
    `sic_description` string COMMENT 'sic_description',
    `address` string COMMENT 'address',
    `city` string COMMENT 'city',
    `state` string COMMENT 'state',
    `zip` string COMMENT 'zip',
    `county` string COMMENT '',
    `phone` string COMMENT '',
    `fax_number` string COMMENT '',
    `website` string COMMENT '',
    `latitude` string COMMENT '',
    `longitude` string COMMENT '',
    `total_employees` string COMMENT '',
    `employee_range` string COMMENT '',
    `sales_volume` string COMMENT '',
    `sales_volume_range` string COMMENT '',
    `contact_firstname` string COMMENT '',
    `contact_lastname` string COMMENT '',
    `contact_fullname` string COMMENT '',
    `contact_gender` string COMMENT '',
    `contact_title` string COMMENT '',
    `contact2_firstname` string COMMENT '',
    `contact2_lastname` string COMMENT '',
    `contact2_fullname` string COMMENT '',
    `contact2_title` string COMMENT '',
    `contact2_gender` string COMMENT '',
    `naics_number` string COMMENT '',
    `industry` string COMMENT '',
    `file` string COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/us_addreasseable_market/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.wellness_coach_gym_id` (
  `gym_id` integer COMMENT 'wellness coach gym_id'
  )
  COMMENT 'Wellness Coach gym_id'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/wellness_coach_gym_id/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.ux_research_partners` (
  `id` integer COMMENT 'Response ID',
  `response_status` string COMMENT 'Response Status',
  `ip_address` string COMMENT 'IP Address',
  `created_at` string COMMENT 'Timestamp (mm/dd/yyyy)',
  `duplicate` string COMMENT 'Duplicate',
  `seconds_taken_to_complete` integer COMMENT 'Time Taken to Complete (Seconds)',
  `country_code` string COMMENT 'Country Code',
  `region` string COMMENT 'Region',
  `country` string COMMENT 'Whats your Country?',
  `industry_category` string COMMENT 'In which industry do you work?',
  `industry_other` string COMMENT 'In which industry do you work?',
  `job_title` string COMMENT 'What is your job most similar to?',
  `job_other` string COMMENT 'What is your job most similar to?',
  `number_of_venues` string COMMENT 'How many venues does the company you work for own?',
  `business_category` string COMMENT 'Which category of business is more similar to the one you work at?',
  `business_other` string COMMENT 'Which category of business is more similar to the one you work at?',
  `price` string COMMENT 'How would you describe the pricing category of the brand you work at?',
  `service_offer_1` string COMMENT 'Which option best describes the type of service the place you work at offers?',
  `service_offer_2` string COMMENT 'Which option best describes the type of service the place you work at offers?',
  `service_offer_3` string COMMENT 'Which option best describes the type of service the place you work at offers?',
  `service_offer_4` string COMMENT 'Which option best describes the type of service the place you work at offers?',
  `service_offer_5` string COMMENT 'Which option best describes the type of service the place you work at offers?',
  `has_cms` string COMMENT 'Does your company use any gym/club management system (CMS, ERP)',
  `cms` string COMMENT 'integration system',
  `degree_in_health_science` string COMMENT 'Do you have a formal degree of education in any health science (physiotherapy, nutrition, physical education, etc.)',
  `has_schedule_software` string COMMENT 'Do you use software developed especially for trainers/coaches to organize your schedule and client information',
  `has_booking_software` string COMMENT 'Do you use software developed especially for trainers/coaches to provide a booking service to your clients',
  `has_specific_software` string COMMENT 'Do you use software developed especially for trainers/coaches to send diet plans, evaluate performance, share personalized training, etc. to your clients',
  `own_equipments` string COMMENT 'Do you own all the equipment you use in your classes',
  `use_fitness_aggregator` string COMMENT 'Do you have a professional relationship with a fitness aggregator',
  `aggregator` string COMMENT 'aggregator name',
  `like_the_most_about_aggregator` string COMMENT 'What do you like the most about your aggregator',
  `like_the_least_about_aggregator` string COMMENT 'What do you like the least about your aggregator',
  `has_online_sevice` string COMMENT 'Do you or the company you work at offer any type of online service',
  `type_of_online_services` string COMMENT 'Which types of online services do you or the company you work at offer',
  `how_hard_is_to_offer_online` string COMMENT 'How hard or easy is the process of offering online options to your clients for you',
  `participate_paid_research_activities` string COMMENT 'Would you like to participate in further paid research activities on this subject in the future',
  `share_company_name` string COMMENT 'Would you like to share your company’s name with us',
  `company_name` string COMMENT 'Company name'
  )
  COMMENT 'Partners UX Research Data'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/ux_research_partners/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file'
);

CREATE EXTERNAL TABLE `analytics.test_table` (
    `id` integer COMMENT ''
)
COMMENT 'test table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/test_table/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.braze_email_verification`(
`person_id` string COMMENT 'person_id',
`email_address` string COMMENT 'email_address',
`email_verification_status` string COMMENT 'email_verification_status'
)
COMMENT ''
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 STORED AS TEXTFILE
 LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/braze_email_verification/'
 TBLPROPERTIES (
     'classification'='csv',
     'skip.header.line.count'='1',
     'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.all_attendances` (
    `dt` string COMMENT 'Date when the checkin occurade',
    `person_id` string COMMENT 'Unique personal id',
    `country_id` integer COMMENT 'Unique country id',
    `partner_id` string COMMENT 'Partner (gym or application partner) unique id',
    `partner_visit_action` string COMMENT 'Description of the offer, such as, wellness, in person, personal trainer, and live classes',
    `checkin_type` string COMMENT '',
    `category` string COMMENT '',
    `data_source` string COMMENT 'Data source of the usage (tagus or old core)'
)
COMMENT ''
STORED AS PARQUET
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gympass/all_attendances/'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='Snappy',
    'typeOfData'='file');

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.gyms_wellness_apps` (
    `gym_id` integer COMMENT 'Gympass replica-full gym id'
)
COMMENT 'Stores only IDs from Gyms marked as a wellness app'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gyms_wellness_apps/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.wellness_usage_events_enriched` (
  `event_id` string COMMENT 'from kafka.wellness_event_publisher_wellness_partner_event_used',
  `usage_received_at` timestamp COMMENT 'event received from parner timestamp',
  `user_id` string COMMENT 'person_id (old core) / user_id (tagus)',
  `client_id` string COMMENT 'company_id (old core)',
  `user_id_source` string COMMENT 'core or tagus',
  `gpw_id` string COMMENT 'user id from gympass_w.user',
  `product_id` string COMMENT 'product id from gympass_w.product',
  `gym_id` integer COMMENT 'old core gym_id',
  `app_slug` string COMMENT 'app slug from gympass_w.product',
  `usage_type` string COMMENT 'type of usage from wellness_event_publisher_wellness_partner_event_used',
  `category_name` string COMMENT 'unique category_name from gympass_w.category',
  `fitness` string COMMENT 'if app is Fintness or Non-fitness',
  `country_code` string COMMENT 'user country code',
  `event_duration` integer COMMENT 'usage duration in minutes from wellness_event_publisher_wellness_partner_event_used',
  `viewing_duration` integer COMMENT 'viewing duration in minutes from wellness_event_publisher_wellness_partner_event_used',
  `plan_active` string COMMENT 'if user has an active plan when the usage occur',
  `plan_monthly_value` decimal(12, 2) COMMENT 'plan monthly value at the day of usage',
  `plan_currency` string COMMENT 'plan currency',
  `plan_purchase_id` string COMMENT 'id from replica_full.person_products',
  `considered_at` timestamp COMMENT 'wellness sessions considered at'
)
COMMENT ''
PARTITIONED BY (
  `dt` string COMMENT 'usage considered day'
)
STORED AS PARQUET
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gympass_wellness/wellness_usage_events_enriched/'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='Snappy',
    'typeOfData'='file');

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.wellness_quote_multi_offer`(
  `multi_offer_id` integer COMMENT 'multi offer unique id',
  `gym_id` integer COMMENT 'gym id to correlate with gym table',
  `product_id` string COMMENT 'wellness product id',
  `offer` string COMMENT 'wellness offer name',
  `offer_id` string COMMENT 'wellness offer id',
  `app_name` string COMMENT 'wellness app name',
  `currency` string COMMENT 'currency app works',
  `free_uses` integer COMMENT 'free uses limitation',
  `capped_per_day` string COMMENT 'capacity limit to pay on day',
  `payment_per_usage` double COMMENT 'payment per usage',
  `monthly_cap` double COMMENT 'capacity of payment per user monthy',
  `country_code` integer COMMENT 'country code',
  `user_location` string COMMENT 'user location',
  `event_type` string COMMENT 'wellness event type',
  `event_duration` integer COMMENT 'duration of the partner offer session',
  `min_unlimited_value` double COMMENT 'minimum user plan unlimited value',
  `max_unlimited_value` double COMMENT 'maximum user plan unlimited value',
  `app_slug` string COMMENT 'wellness app slug'
  )
  COMMENT 'The data rules to use on payment calculation to the multi offer wellness partners'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/gympass_wellness/multi_offer_conditions/'
    TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file');

CREATE EXTERNAL TABLE `analytics.v_dim_transactions_tagus` (
    `data_source` string COMMENT '',
    `person_cart_id` string COMMENT '',
    `person_id` string COMMENT '',
    `purchase_person_id` string COMMENT '',
    `company_id` string COMMENT '',
    `date` integer COMMENT '',
    `date_hour` integer COMMENT '',
    `refund_date` integer COMMENT '',
    `action` string COMMENT '',
    `gym_id` string COMMENT '',
    `country_id` integer COMMENT '',
    `payment_method` string COMMENT '',
    `payment_type` string COMMENT '',
    `enrollment_type` string COMMENT '',
    `gym_product_value` decimal(12, 2) COMMENT '',
    `token_group_type` string COMMENT '',
    `active_plan_value` decimal(12, 2) COMMENT '',
    `sales_value` decimal(12, 2) COMMENT '',
    `revenue_value` decimal(12, 2) COMMENT '',
    `gym_expenses_value` decimal(12, 2) COMMENT '',
    `refund_value` decimal(12, 2) COMMENT '',
    `valid_start_day` integer COMMENT '',
    `valid_end_day` integer COMMENT '',
    `valid_end_day_adjusted` integer COMMENT '',
    `valid_days_curr_month` integer COMMENT '',
    `valid_days_next_month` integer COMMENT '',
    `total_price` decimal(12, 2) COMMENT '',
    `partner_visit_action` string COMMENT '',
    `manual_inputs_description` string COMMENT '',
    `original_adjustment_date` integer COMMENT '',
    `adjustment_comments` string COMMENT ''
)
COMMENT ''
STORED AS PARQUET
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/edw_tagus/v_dim_transactions_tagus/'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='Snappy',
    'typeOfData'='file');

CREATE EXTERNAL TABLE `analytics.braze_summary_table` (
    `person_id` integer COMMENT '',
    `external_user_id` string COMMENT '',
    `event` string COMMENT '',
    `channel` string COMMENT '',
    `event_date` date COMMENT '',
    `event_day` integer COMMENT '',
    `timestamp_utc` timestamp COMMENT '',
    `timestamp_local` timestamp COMMENT '',
    `timezone` string COMMENT '',
    `setup_type` string COMMENT '',
    `campaign_title` string COMMENT '',
    `campaign_id` string COMMENT '',
    `variation_id` string COMMENT '',
    `canvas_variation_name` string COMMENT '',
    `canvas_step_id` string COMMENT '',
    `canvas_step_name` string COMMENT '',
    `url` string COMMENT '',
    `platform` string COMMENT '',
    `user_agent` string COMMENT '',
    `device_id` string COMMENT '',
    `button_id` string COMMENT '',
    `bounce_reason` string COMMENT '',
    `target_audience` string COMMENT '',
    `campaign_country` string COMMENT '',
    `user_status` string COMMENT '',
    `company_id` integer COMMENT '',
    `parent_company_id` integer COMMENT '',
    `relationship_title` string COMMENT '',
    `parent_company_title` string COMMENT '',
    `family_member` boolean COMMENT '',
    `country_title` string COMMENT '',
    `bu` integer COMMENT '',
    `contact_permission` string COMMENT ''
    )
COMMENT ''
STORED AS PARQUET
LOCATION
  's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/braze_summary_table/'
TBLPROPERTIES (
  'classification'='parquet',
  'compressionType'='Snappy',
  'typeOfData'='file');

CREATE EXTERNAL TABLE `analytics.braze_attribution`(
    `conversion_type` string COMMENT '',
    `conversion_date` date COMMENT '',
    `current_plan_level` string COMMENT '',
    `free_plan_purchase` boolean COMMENT '',
    `event` string COMMENT '',
    `event_date` date COMMENT '',
    `days_between` bigint COMMENT '',
    `interaction` boolean COMMENT '',
    `attributed_interaction` boolean COMMENT '',
    `interaction_order` bigint COMMENT '',
    `attributed_touch` boolean COMMENT '',
    `touch_order` bigint COMMENT '',
    `channel` string COMMENT '',
    `timestamp_utc` timestamp COMMENT '',
    `setup_type` string COMMENT '',
    `target_audience` string COMMENT '',
    `campaign_title` string COMMENT '',
    `campaign_id` string COMMENT '',
    `variation_id` string COMMENT '',
    `canvas_variation_name` string COMMENT '',
    `canvas_step_id` string COMMENT '',
    `canvas_step_name` string COMMENT '',
    `url` string COMMENT '',
    `person_id` integer COMMENT '',
    `company_id` integer COMMENT '',
    `parent_company_id` integer COMMENT '',
    `parent_company_title` string COMMENT '',
    `relationship_title` string COMMENT '',
    `family_member` boolean COMMENT '',
    `country_title` string COMMENT '',
    `bu` integer COMMENT '',
    `contact_permission` string COMMENT ''
    )
COMMENT ''
STORED AS PARQUET
LOCATION
  's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/braze_attribution/'
TBLPROPERTIES (
  'classification'='parquet',
  'compressionType'='Snappy',
  'typeOfData'='file');

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.edw_transaction_fixes_mrr` (
    `data_source` string COMMENT '',
    `person_cart_id` integer COMMENT '',
    `person_id` integer COMMENT '',
    `purchase_person_id` integer COMMENT '',
    `company_id` integer COMMENT '',
    `date` integer COMMENT '',
    `date_hour` integer COMMENT '',
    `refund_date` integer COMMENT '',
    `action` string COMMENT '',
    `gym_id` integer COMMENT '',
    `country_id` integer COMMENT '',
    `payment_method` string COMMENT '',
    `payment_type` string COMMENT '',
    `enrollment_type` string COMMENT '',
    `gym_product_value` decimal(12, 2) COMMENT '',
    `token_group_type` string COMMENT '',
    `active_plan_value` decimal(12, 2) COMMENT '',
    `sales_value` decimal(12, 2) COMMENT '',
    `revenue_value` decimal(12, 2) COMMENT '',
    `gym_expenses_value` decimal(12, 2) COMMENT '',
    `refund_value` decimal(12, 2) COMMENT '',
    `valid_start_day` integer COMMENT '',
    `valid_end_day` integer COMMENT '',
    `valid_end_day_adjusted` integer COMMENT '',
    `valid_days_curr_month` integer COMMENT '',
    `valid_days_next_month` integer COMMENT '',
    `total_price` decimal(12, 2) COMMENT '',
    `partner_visit_action` string COMMENT '',
    `manual_inputs_description` string COMMENT '',
    `original_adjustment_date` integer COMMENT '',
    `adjustment_comments` string COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/edw_transaction_fixes_mrr/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.zip_info`(
`zip` integer COMMENT 'zipcode',
`lat` float COMMENT 'latitude of zipcode',
`lng` float COMMENT 'longitude of zipcode'
)
COMMENT ''
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 STORED AS TEXTFILE
 LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/risk_analytics/zip_codes/'
 TBLPROPERTIES (
     'classification'='csv',
     'skip.header.line.count'='1',
     'typeOfData'='file'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `analytics.netsuite_cost_center` (
    `netsuite_id` string COMMENT '',
    `active_flag` string COMMENT '',
    `external_id` string COMMENT '',
    `cost_center_hierarchy_name` string COMMENT '',
    `cost_center_name` string COMMENT '',
    `cost_center_num` string COMMENT '',
    `as_of` string COMMENT ''
)
COMMENT ''
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_ANALYTICS_TEAM__/hive_analytics/netsuite_cost_center/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);
