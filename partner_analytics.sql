# __   _    _  _  _ ___   _____ _____ __  _ _  ___  ___   ___  _ _  _  __   __  _   _
#|  \ / \  | \| |/ \_ _| | __\ V / __/ _|| | ||_ _|| __| |_ _|| U || |/ _| / _|/ \ | |
#| o | o ) | \\ ( o ) |  | _| ) (| _( (_ | U | | | | _|   | | |   || |\_ \ \_ ( o )| |_
#|__/ \_/  |_|\_|\_/|_|  |___/_n_\___\__||___| |_| |___|  |_| |_n_||_||__/ |__/\_,7|___|
# *use Makefile

CREATE DATABASE IF NOT EXISTS `partner_analytics`;

CREATE EXTERNAL TABLE IF NOT EXISTS `partner_analytics.partners_migration` (
    `gym_id` integer COMMENT 'Old Core Gym Identifier.',
    `partner_id` string COMMENT 'Tagus Identifier.',
    `partner_name` string COMMENT 'Partner name.',
    `gym_network_id` integer COMMENT 'From which network id this partner belongs.',
    `gym_network_flag` boolean COMMENT 'If the gym is part of a network.',
    `wishlist_flag` boolean COMMENT 'If the partner is wishlist.',
    `partner_active_flag` boolean COMMENT 'Partner has at least one validated checkin in the last 30 days',
    `network_size` integer COMMENT 'The size of the network that this partner is included.',
    `network_size_range` string COMMENT 'Ranges of the sizes of the network that this partner is included.',
    `country_code` string COMMENT 'Country ID is the identifier of a country that the Partner belongs to.',
    `state` string COMMENT 'From which state this partner is.',
    `city` string COMMENT 'From which city this partner is',
    `number_of_validations_avg` decimal(12, 2) COMMENT 'Average number of validations on this partner in the last 4 months.',
    `number_of_validations_range` string COMMENT 'Ranges of the avarage number of validations on this partner in the last 4 months.',
    `last_contract_signed_at` timestamp COMMENT 'The last time that this partner received a contract on the old core.',
    `contract_files_count` integer COMMENT 'Number of files from last signed contract.',
    `number_active_products` integer COMMENT 'Number of active products from this partner.',
    `active_products` string COMMENT 'Number of active products from this partner.',
    `max_weekly_uses` integer COMMENT 'Max weekly uses, between all products, setted on the old core.',
    `max_monthly_uses` integer COMMENT 'Max monthly uses, between all products, setted on the old core.',
    `legal_document` string COMMENT 'Legal number document from gyms.',
    `banking_documents` string COMMENT 'Legal number documents from the bank_accounts.',
    `account_salesforce_id` string COMMENT 'Salesforce Account ID from the partner',
    `seller_salesforce_id` string COMMENT 'Salesforce ID from the Seller.',
    `seller_email` string COMMENT 'Seller email.',
    `link` string COMMENT 'Direct link of this partner from the old core.',
    `tagus_flag` boolean COMMENT 'If this partner is on tagus.',
    `address_flag` boolean COMMENT 'If this partner has an address.',
    `personal_trainer_flag` boolean COMMENT 'If this partner is a personal trainer.',
    `partner_app_flag` boolean COMMENT 'If this partner is a wellness app.',
    `ladder_flag` boolean COMMENT 'If this partner has ladder applied in any product.',
    `any_virtual_product_flag` boolean COMMENT 'If any of the products is a virtual one',
    `product_restrictions_flag` boolean COMMENT 'If this partner has some product with product restriction.',
    `commercial_conditions_flag` boolean COMMENT 'If this partner has any commercial conditions.',
    `minimum_guaranteed_flag` boolean COMMENT 'If this partner has volume guaranteed amount setted.',
    `advanced_payment_flag` boolean COMMENT 'If this partner has prepayment amount setted.',
    `smart_check_in_flag` boolean COMMENT 'If this partner uses smart check-in.',
    `webhook_flag` boolean COMMENT 'If the partner has some event on webhook.',
    `app_integration_flag` boolean COMMENT 'If this partner has app events on the webhooks.',
    `booking_integration_flag` boolean COMMENT 'If this partner has booking events on the webhooks.',
    `checkin_integration_flag` boolean COMMENT 'If this partner has check-in events on the webhooks.',
    `booking_classes_flag` boolean COMMENT 'If the partner has booking classes.',
    `booking_or_webhook_integration_flag` boolean COMMENT 'If the partner has integration by booking or webhooks.'
)
COMMENT 'Partners Migration Table'
STORED AS PARQUET
LOCATION 's3://__BUCKET_DATA_LAKE_STRUCTURED__/partner_analytics/partners_migration/'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='Snappy',
    'typeOfData'='file');

CREATE EXTERNAL TABLE IF NOT EXISTS `partner_analytics.flat_booking` (
    `gym_id` integer COMMENT 'Old Core Gym Identifier.',
    `partner_name` string COMMENT 'Partner name.',
    `country_code` string COMMENT 'Country ID is the identifier of a country that the Partner belongs to.',
    `partner_timezone` string COMMENT 'Timezone from that partner.',
    `in_person_flag` boolean COMMENT 'If this slot is from a product that is in person.',
    `live_class_flag` boolean COMMENT 'If this slot is a live class.',
    `personal_trainer_flag` boolean COMMENT 'If this slot is from a personal trainer.',
    `slot_id` integer COMMENT 'Class slot id.',
    `class_id` integer COMMENT 'Class id.',
    `product_id` integer COMMENT 'Product id from this slot.',
    `length_in_minutes` integer COMMENT 'The length in minutes from this class slot.',
    `total_capacity` integer COMMENT 'The total capacity of people from this class slot.',
    `virtual_slot_flag` boolean COMMENT 'If this slot is a virtual one.',
    `class_start_date_at` timestamp COMMENT 'Date and time when occurs the class at UTC time.',
    `class_start_date_localtime` timestamp COMMENT 'Date and time when occurs the class at local time.',
    `class_slot_created_at` timestamp COMMENT 'Date and time when the class was created.',
    `class_slot_cancellable_until` timestamp COMMENT 'Date and time when the class was canceled.',
    `class_slot_deleted_at` timestamp COMMENT 'Date and time when the class was deleted.',
    `class_name` string COMMENT 'Class name.',
    `category_name` string COMMENT 'Category name.',
    `category_locale` string COMMENT 'Partner locale.',
    `client_system_id` string COMMENT 'System id.',
    `cms_slug` string COMMENT 'CMS slug.',
    `cms_name` string COMMENT 'CMS name.',
    `class_description` string COMMENT 'Class description.',
    `user_gympass_id` string COMMENT 'User unique token.',
    `person_id` integer COMMENT 'old core person id',
    `user_id` string COMMENT 'tagus user id',
    `booking_id` integer COMMENT 'Booking unique identifier.',
    `booking_reference_number` string COMMENT 'Booking reference number',
    `booking_status` string COMMENT 'Booking status.',
    `booking_created_at` timestamp COMMENT 'Booking created date',
    `booking_updated_at` timestamp COMMENT 'Booking status updated at',
    `person_transaction_id` integer COMMENT 'Person transaction id related.',
    `unique_attendance_identifier` string COMMENT 'Unique attendance identifier for that booking class.',
    `no_show_value` DECIMAL(13, 4) COMMENT 'Penalty cost in case of no show.',
    `late_cancel_value` DECIMAL(13, 4) COMMENT 'Penalty cost in case of late cancel.',
    `monthly_value` DECIMAL(13, 4) COMMENT 'Monthly value from the product.',
    `unit_price` DECIMAL(13, 4) COMMENT 'Unit price from the product.',
    `currency_id` integer COMMENT 'Currency id related.',
    `max_monthly_uses` integer COMMENT 'Max monthly uses, between all products, setted on the old core.',
    `max_weekly_uses` integer COMMENT 'Max weekly uses, between all products, setted on the old core.,',
    `product_created_at` timestamp COMMENT 'Product created date.',
    `product_deleted_at` timestamp COMMENT 'Product deleted date.',
    `reference_date` string COMMENT 'Reference from the occur date but tidy with the timezone.'
)
COMMENT 'Flat Booking'
STORED AS PARQUET
LOCATION 's3://__BUCKET_DATA_LAKE_STRUCTURED__/partner_analytics/flat_booking/'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='Snappy',
    'typeOfData'='file');


CREATE EXTERNAL TABLE `partner_analytics.gyms_image_rate`(
  `image_rate` double COMMENT 'image rate',
  `gym_id` integer COMMENT 'gym_id',
  `created_at` timestamp COMMENT 'date when the image rating was created',
  `is_current_version` boolean COMMENT 'flag indicating if the image rating is the latest version'
)
COMMENT 'Gyms image rate'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://__BUCKET_DATA_LAKE_STRUCTURED__/partner_analytics/gyms_image_rate/'
TBLPROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'typeOfData'='file'
);


CREATE EXTERNAL TABLE `partner_analytics.flat_attendances`(
    `user_id` string COMMENT 'tagus user id',
    `partner_id` string COMMENT 'tagus partner id',
    `product_id` string COMMENT 'tagus product id',
    `gympass_id` bigint COMMENT 'gympass id | person unique token',
    `gym_product_id` integer COMMENT 'old core product id',
    `checkin_type` string COMMENT 'checkin type',
    `product_is_live_class` boolean COMMENT 'is live class',
    `slot_id` integer COMMENT 'booking slot id',
    `plan_id` string COMMENT 'tagus plan id',
    `attendance_type` string COMMENT 'attendance type value',
    `is_attendance` boolean COMMENT 'attendance was validated',
    `promise_attendance_id` bigint COMMENT 'promise attendance id',
    `attendance_id` bigint COMMENT 'attendance id',
    `attendance_type_id` integer COMMENT 'attendance type id',
    `attendance_details_id` bigint COMMENT 'attendance details id',
    `transactional_uid` string COMMENT 'transacional uid',
    `order_detail_id` string COMMENT 'tagus user order id',
    `booking_reference_number` string COMMENT 'booking reference number',
    `booking_status` string COMMENT 'booking status',
    `created_at` timestamp COMMENT 'promise created at (UTC)',
    `class_occur_at` timestamp COMMENT 'class started at (UTC)',
    `canceled_at` timestamp COMMENT 'promise canceled at (UTC)',
    `considered_at` timestamp COMMENT 'attendance considered at (UTC)',
    `considered_at_tz` timestamp COMMENT 'attendance considered at (local timezone)',
    `issue_reported_at` timestamp COMMENT 'issue reported at (UTC)',
    `checkin_validated_at` timestamp COMMENT 'checking validate at (UTC)',
    `booking_status_updated_at` timestamp COMMENT 'booking status updated at (UTC)',
    `timezone` string COMMENT 'timezone',
    `plan_monthly_price` decimal(12, 2) COMMENT 'tagus plan monthly price',
    `plan_currency_id` string COMMENT 'tagus plan currency id',
    `reference_date` string COMMENT 'Reference from the occur date but tidy with the timezone.'
)
COMMENT 'Flat Attendances'
STORED AS PARQUET
LOCATION 's3://__BUCKET_DATA_LAKE_STRUCTURED__/partner_analytics/flat_attendances/'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='Snappy',
    'typeOfData'='file');
