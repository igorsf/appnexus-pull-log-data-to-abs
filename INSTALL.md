
# Create a SQL Data Warehouse.
https://docs.microsoft.com/en-us/azure/sql-data-warehouse/sql-data-warehouse-get-started-provision

# Create a credentials, external data source, file format

```sql
CREATE MASTER KEY;

CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH
    IDENTITY = 'user',
    SECRET = '<azure_storage_account_key>'
;

CREATE EXTERNAL DATA SOURCE AzureStorage
WITH (
    TYPE = HADOOP,
    LOCATION = 'wasbs://<blob_container_name>@<azure_storage_account_name>.blob.core.windows.net',
    CREDENTIAL = AzureStorageCredential
);

CREATE EXTERNAL FILE FORMAT TabDelimitedFile
WITH
(
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS
    (
        FIELD_TERMINATOR = '\t',
        DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss',
        USE_TYPE_DEFAULT = FALSE
    ),
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.GzipCodec'
);
```

# Create the external tables.

```sql
CREATE SCHEMA [asb]
GO

--StandardFeed
CREATE EXTERNAL TABLE [asb].StandardFeedV1 (
    [auction_id_64] [bigint] NULL,
    [timestamp] [datetime] NULL,
    [user_tz_offset] [smallint] NULL,
    [creative_width] [smallint] NULL,
    [creative_height] [smallint] NULL,
    [media_type] [tinyint] NULL,
    [fold_position] [tinyint] NULL,
    [event_type] [nvarchar](10) NOT NULL,
    [imp_type] [tinyint] NULL,
    [payment_type] [tinyint] NULL,
    [media_cost_dollars_cpm] [numeric](18,6) NULL,
    [revenue_type] [smallint] NULL,
    [buyer_spend] [numeric](18,6) NULL,
    [buyer-bid] [numeric](18,6) NULL,
    [ecp] [numeric](18,6) NULL,
    [eap] [numeric](18,6) NULL,
    [is_imp] [int] NULL,
    [is_learn] [tinyint] NULL,
    [predict_type_rev] [smallint] NULL,
    [user_id_64] [bigint] NULL,
    [ip_address] [nvarchar](40) NULL,
    [ip_address_trunc] [nvarchar](40) NULL,
    [geo_country] [char](2) NULL,
    [geo_region] [char](2) NULL,
    [operating_system] [tinyint] NULL,
    [browser] [tinyint] NULL,
    [language] [tinyint] NULL,
    [venue_id] [int] NULL,
    [seller_member_id] [int] NULL,
    [publisher_id] [nvarchar](20) NULL,
    [site_id] [nvarchar](20) NULL,
    [site_domain] [nvarchar](100) NULL,
    [tag_id] [nvarchar](20) NULL,
    [external_inv_id] [nvarchar](20) NULL,
    [reserve_price] [nvarchar](20) NULL,
    [seller_revenue_cpm] [nvarchar](20) NULL,
    [media_buy_rev_share_pct] [nvarchar](20) NULL,
    [pub_rule_id] [nvarchar](20) NULL,
    [seller_currency] [nvarchar](10) NULL,
    [publisher_currency] [nvarchar](10) NULL,
    [publisher_exchange_rate] [nvarchar](20) NULL,
    [serving_fees_cpm] [numeric](18,6) NULL,
    [serving_fees_revshare] [nvarchar](20) NULL,
    [buyer_member_id] [int] NULL,
    [advertiser_id] [int] NULL,
    [brand_id] [int] NULL,
    [advertiser_frequency] [int] NULL,
    [advertiser_recency] [int] NULL,
    [insertion_order_id] [int] NULL,
    [campaign_group_id] [int] NULL,
    [campaign_id] [int] NULL,
    [creative_id] [int] NULL,
    [creative_freq] [int] NULL,
    [creative_rec] [int] NULL,
    [cadence_modifier] [numeric](18,6) NULL,
    [can_convert] [tinyint] NULL,
    [user_group_id] [int] NULL,
    [is_control] [int] NULL,
    [controller_pct] [numeric](18,6) NULL,
    [controller_creative_pct] [int] NULL,
    [is_click] [int] NULL,
    [pixel_id] [int] NULL,
    [is_remarketing] [tinyint] NULL,
    [post_click_conv] [int] NULL,
    [post_view_conv] [int] NULL,
    [post_click_revenue] [numeric](18,6) NULL,
    [post_view_revenue] [numeric](18,6) NULL,
    [order_id] [nvarchar](36) NULL,
    [external_data] [nvarchar](30) NULL,
    [pricing_type] [char](3) NULL,
    [booked_revenue_dollars] [numeric](18,6) NULL,
    [booked_revenue_adv_curr] [numeric](18,6) NULL,
    [commission_cpm] [numeric](18,6) NULL,
    [commission_revshare] [numeric](18,6) NULL,
    [auction_service_deduction] [numeric](18,6) NULL,
    [auction_service_fees] [numeric](18,6) NULL,
    [creative_overage_fees] [numeric](18,6) NULL,
    [clear_fees] [numeric](18,6) NULL,
    [buyer_currency] [char](3) NULL,
    [advertiser_currency] [nvarchar](10) NULL,
    [advertiser_exchange_rate] [numeric](18,6) NULL,
    [latitude] [nvarchar](20) NULL,
    [longitude] [nvarchar](20) NULL,
    [device_unique_id] [nvarchar](100) NULL,
    [device_id] [int] NULL,
    [carrier_id] [int] NULL,
    [deal_id] [int] NULL,
    [view_result] [nvarchar](400) NULL,
    [application_id] [nvarchar](400) NULL,
    [supply_type] [nvarchar](40) NULL,
    [sdk_version] [nvarchar](40) NULL,
    [ozone_id] [nvarchar](20) NULL,
    [billing_period_id] [int] NULL,
    [view_non_measurable_reason] [int] NULL,
    [external_uid] [nvarchar](100) NULL,
    [request_uuid] [nvarchar](36) NULL,
    [geo_dma] [int] NULL,
    [geo_city] [int] NULL,
    [mobile_app_instance_id] [nvarchar](20) NULL,
    [traffic_source_code] [nvarchar](100) NULL,
    [external_request_id] [nvarchar](100) NULL,
    [deal_type] [int] NULL,
    [ym_floor_id] [nvarchar](20) NULL,
    [ym_bias_id] [nvarchar](20) NULL,
    [is_filtered_request] [nvarchar](20) NULL,
    [age] [int] NULL,
    [gender] [char](1) NULL,
    [is_exclusive] [nvarchar](20) NULL,
    [bid_priority] [nvarchar](20) NULL,
    [custom_model_id] [int] NULL,
    [custom_model_last_modified] [int] NULL,
    [leaf_name] [nvarchar](20) NULL,
    [data_costs_cpm] [numeric](18,6) NULL,
    [device_type] [int] NULL
)
WITH
(
    LOCATION='/standard/'
,   DATA_SOURCE = AzureStorage
,   FILE_FORMAT = TabDelimitedFile
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
)
;

-- new column geo_postal_code
CREATE EXTERNAL TABLE [asb].StandardFeedV2 (
    [auction_id_64] [bigint] NULL,
    [timestamp] [datetime] NULL,
    [user_tz_offset] [smallint] NULL,
    [creative_width] [smallint] NULL,
    [creative_height] [smallint] NULL,
    [media_type] [tinyint] NULL,
    [fold_position] [tinyint] NULL,
    [event_type] [nvarchar](10) NOT NULL,
    [imp_type] [tinyint] NULL,
    [payment_type] [tinyint] NULL,
    [media_cost_dollars_cpm] [numeric](18,6) NULL,
    [revenue_type] [smallint] NULL,
    [buyer_spend] [numeric](18,6) NULL,
    [buyer-bid] [numeric](18,6) NULL,
    [ecp] [numeric](18,6) NULL,
    [eap] [numeric](18,6) NULL,
    [is_imp] [int] NULL,
    [is_learn] [tinyint] NULL,
    [predict_type_rev] [smallint] NULL,
    [user_id_64] [bigint] NULL,
    [ip_address] [nvarchar](40) NULL,
    [ip_address_trunc] [nvarchar](40) NULL,
    [geo_country] [char](2) NULL,
    [geo_region] [char](2) NULL,
    [operating_system] [tinyint] NULL,
    [browser] [tinyint] NULL,
    [language] [tinyint] NULL,
    [venue_id] [int] NULL,
    [seller_member_id] [int] NULL,
    [publisher_id] [nvarchar](20) NULL,
    [site_id] [nvarchar](20) NULL,
    [site_domain] [nvarchar](100) NULL,
    [tag_id] [nvarchar](20) NULL,
    [external_inv_id] [nvarchar](20) NULL,
    [reserve_price] [nvarchar](20) NULL,
    [seller_revenue_cpm] [nvarchar](20) NULL,
    [media_buy_rev_share_pct] [nvarchar](20) NULL,
    [pub_rule_id] [nvarchar](20) NULL,
    [seller_currency] [nvarchar](10) NULL,
    [publisher_currency] [nvarchar](10) NULL,
    [publisher_exchange_rate] [nvarchar](20) NULL,
    [serving_fees_cpm] [numeric](18,6) NULL,
    [serving_fees_revshare] [nvarchar](20) NULL,
    [buyer_member_id] [int] NULL,
    [advertiser_id] [int] NULL,
    [brand_id] [int] NULL,
    [advertiser_frequency] [int] NULL,
    [advertiser_recency] [int] NULL,
    [insertion_order_id] [int] NULL,
    [campaign_group_id] [int] NULL,
    [campaign_id] [int] NULL,
    [creative_id] [int] NULL,
    [creative_freq] [int] NULL,
    [creative_rec] [int] NULL,
    [cadence_modifier] [numeric](18,6) NULL,
    [can_convert] [tinyint] NULL,
    [user_group_id] [int] NULL,
    [is_control] [int] NULL,
    [controller_pct] [numeric](18,6) NULL,
    [controller_creative_pct] [int] NULL,
    [is_click] [int] NULL,
    [pixel_id] [int] NULL,
    [is_remarketing] [tinyint] NULL,
    [post_click_conv] [int] NULL,
    [post_view_conv] [int] NULL,
    [post_click_revenue] [numeric](18,6) NULL,
    [post_view_revenue] [numeric](18,6) NULL,
    [order_id] [nvarchar](36) NULL,
    [external_data] [nvarchar](30) NULL,
    [pricing_type] [char](3) NULL,
    [booked_revenue_dollars] [numeric](18,6) NULL,
    [booked_revenue_adv_curr] [numeric](18,6) NULL,
    [commission_cpm] [numeric](18,6) NULL,
    [commission_revshare] [numeric](18,6) NULL,
    [auction_service_deduction] [numeric](18,6) NULL,
    [auction_service_fees] [numeric](18,6) NULL,
    [creative_overage_fees] [numeric](18,6) NULL,
    [clear_fees] [numeric](18,6) NULL,
    [buyer_currency] [char](3) NULL,
    [advertiser_currency] [nvarchar](10) NULL,
    [advertiser_exchange_rate] [numeric](18,6) NULL,
    [latitude] [nvarchar](20) NULL,
    [longitude] [nvarchar](20) NULL,
    [device_unique_id] [nvarchar](100) NULL,
    [device_id] [int] NULL,
    [carrier_id] [int] NULL,
    [deal_id] [int] NULL,
    [view_result] [nvarchar](400) NULL,
    [application_id] [nvarchar](400) NULL,
    [supply_type] [nvarchar](40) NULL,
    [sdk_version] [nvarchar](40) NULL,
    [ozone_id] [nvarchar](20) NULL,
    [billing_period_id] [int] NULL,
    [view_non_measurable_reason] [int] NULL,
    [external_uid] [nvarchar](100) NULL,
    [request_uuid] [nvarchar](36) NULL,
    [geo_dma] [int] NULL,
    [geo_city] [int] NULL,
    [mobile_app_instance_id] [nvarchar](20) NULL,
    [traffic_source_code] [nvarchar](100) NULL,
    [external_request_id] [nvarchar](100) NULL,
    [deal_type] [int] NULL,
    [ym_floor_id] [nvarchar](20) NULL,
    [ym_bias_id] [nvarchar](20) NULL,
    [is_filtered_request] [nvarchar](20) NULL,
    [age] [int] NULL,
    [gender] [char](1) NULL,
    [is_exclusive] [nvarchar](20) NULL,
    [bid_priority] [nvarchar](20) NULL,
    [custom_model_id] [int] NULL,
    [custom_model_last_modified] [int] NULL,
    [leaf_name] [nvarchar](20) NULL,
    [data_costs_cpm] [numeric](18,6) NULL,
    [device_type] [int] NULL,
    [geo_postal_code] [nvarchar](20) NULL
)
WITH
(
    LOCATION='/standard_v2/'
,   DATA_SOURCE = AzureStorage
,   FILE_FORMAT = TabDelimitedFile
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
)
;

-- new column geo_postal_code
CREATE EXTERNAL TABLE [asb].StandardFeedV3 (
    [auction_id_64] [bigint] NULL,
    [timestamp] [datetime] NULL,
    [user_tz_offset] [smallint] NULL,
    [creative_width] [smallint] NULL,
    [creative_height] [smallint] NULL,
    [media_type] [tinyint] NULL,
    [fold_position] [tinyint] NULL,
    [event_type] [nvarchar](10) NOT NULL,
    [imp_type] [tinyint] NULL,
    [payment_type] [tinyint] NULL,
    [media_cost_dollars_cpm] [numeric](18,6) NULL,
    [revenue_type] [smallint] NULL,
    [buyer_spend] [numeric](18,6) NULL,
    [buyer-bid] [numeric](18,6) NULL,
    [ecp] [numeric](18,6) NULL,
    [eap] [numeric](18,6) NULL,
    [is_imp] [int] NULL,
    [is_learn] [tinyint] NULL,
    [predict_type_rev] [smallint] NULL,
    [user_id_64] [bigint] NULL,
    [ip_address] [nvarchar](40) NULL,
    [ip_address_trunc] [nvarchar](40) NULL,
    [geo_country] [char](2) NULL,
    [geo_region] [char](2) NULL,
    [operating_system] [tinyint] NULL,
    [browser] [tinyint] NULL,
    [language] [tinyint] NULL,
    [venue_id] [int] NULL,
    [seller_member_id] [int] NULL,
    [publisher_id] [nvarchar](20) NULL,
    [site_id] [nvarchar](20) NULL,
    [site_domain] [nvarchar](100) NULL,
    [tag_id] [nvarchar](20) NULL,
    [external_inv_id] [nvarchar](20) NULL,
    [reserve_price] [nvarchar](20) NULL,
    [seller_revenue_cpm] [nvarchar](20) NULL,
    [media_buy_rev_share_pct] [nvarchar](20) NULL,
    [pub_rule_id] [nvarchar](20) NULL,
    [seller_currency] [nvarchar](10) NULL,
    [publisher_currency] [nvarchar](10) NULL,
    [publisher_exchange_rate] [nvarchar](20) NULL,
    [serving_fees_cpm] [numeric](18,6) NULL,
    [serving_fees_revshare] [nvarchar](20) NULL,
    [buyer_member_id] [int] NULL,
    [advertiser_id] [int] NULL,
    [brand_id] [int] NULL,
    [advertiser_frequency] [int] NULL,
    [advertiser_recency] [int] NULL,
    [insertion_order_id] [int] NULL,
    [campaign_group_id] [int] NULL,
    [campaign_id] [int] NULL,
    [creative_id] [int] NULL,
    [creative_freq] [int] NULL,
    [creative_rec] [int] NULL,
    [cadence_modifier] [numeric](18,6) NULL,
    [can_convert] [tinyint] NULL,
    [user_group_id] [int] NULL,
    [is_control] [int] NULL,
    [controller_pct] [numeric](18,6) NULL,
    [controller_creative_pct] [int] NULL,
    [is_click] [int] NULL,
    [pixel_id] [int] NULL,
    [is_remarketing] [tinyint] NULL,
    [post_click_conv] [int] NULL,
    [post_view_conv] [int] NULL,
    [post_click_revenue] [numeric](18,6) NULL,
    [post_view_revenue] [numeric](18,6) NULL,
    [order_id] [nvarchar](36) NULL,
    [external_data] [nvarchar](30) NULL,
    [pricing_type] [char](3) NULL,
    [booked_revenue_dollars] [numeric](18,6) NULL,
    [booked_revenue_adv_curr] [numeric](18,6) NULL,
    [commission_cpm] [numeric](18,6) NULL,
    [commission_revshare] [numeric](18,6) NULL,
    [auction_service_deduction] [numeric](18,6) NULL,
    [auction_service_fees] [numeric](18,6) NULL,
    [creative_overage_fees] [numeric](18,6) NULL,
    [clear_fees] [numeric](18,6) NULL,
    [buyer_currency] [char](3) NULL,
    [advertiser_currency] [nvarchar](10) NULL,
    [advertiser_exchange_rate] [numeric](18,6) NULL,
    [latitude] [nvarchar](20) NULL,
    [longitude] [nvarchar](20) NULL,
    [device_unique_id] [nvarchar](100) NULL,
    [device_id] [int] NULL,
    [carrier_id] [int] NULL,
    [deal_id] [int] NULL,
    [view_result] [nvarchar](400) NULL,
    [application_id] [nvarchar](400) NULL,
    [supply_type] [nvarchar](40) NULL,
    [sdk_version] [nvarchar](40) NULL,
    [ozone_id] [nvarchar](20) NULL,
    [billing_period_id] [int] NULL,
    [view_non_measurable_reason] [int] NULL,
    [external_uid] [nvarchar](100) NULL,
    [request_uuid] [nvarchar](36) NULL,
    [geo_dma] [int] NULL,
    [geo_city] [int] NULL,
    [mobile_app_instance_id] [nvarchar](20) NULL,
    [traffic_source_code] [nvarchar](100) NULL,
    [external_request_id] [nvarchar](100) NULL,
    [deal_type] [int] NULL,
    [ym_floor_id] [nvarchar](20) NULL,
    [ym_bias_id] [nvarchar](20) NULL,
    [is_filtered_request] [nvarchar](20) NULL,
    [age] [int] NULL,
    [gender] [char](1) NULL,
    [is_exclusive] [nvarchar](20) NULL,
    [bid_priority] [nvarchar](20) NULL,
    [custom_model_id] [int] NULL,
    [custom_model_last_modified] [int] NULL,
    [leaf_name] [nvarchar](20) NULL,
    [data_costs_cpm] [numeric](18,6) NULL,
    [device_type] [int] NULL,
    [geo_postal_code] [nvarchar](20) NULL,
    [imps_for_budget_caps_pacing] [int] NULL   
)
WITH
(
    LOCATION='/standard_v3/'
,   DATA_SOURCE = AzureStorage
,   FILE_FORMAT = TabDelimitedFile
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
)
;

-- Create new schema for appnexus tables
CREATE SCHEMA [apn]
GO

-- Load the data into new tables
CREATE TABLE [apn].[StandardFeedV1] 
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)
AS SELECT * FROM [asb].[StandardFeedV1]
OPTION (LABEL = 'CTAS : Load [apn].[StandardFeedV1]');

CREATE TABLE [apn].[StandardFeedV2] 
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)
AS SELECT * FROM [asb].[StandardFeedV2]
OPTION (LABEL = 'CTAS : Load [apn].[StandardFeedV2]');

-- To see a particular request identified by its label
SELECT * FROM sys.dm_pdw_exec_requests as r
WHERE r.[label] = 'CTAS : Load [apn].[StandardFeedV1]'
      OR r.[label] = 'CTAS : Load [apn].[StandardFeedV2]';

CREATE VIEW [asb].[StandardFeed] AS 
SELECT * FROM [apn].StandardFeedV1 
UNION ALL 
SELECT [auction_id_64], [timestamp], [user_tz_offset], [creative_width], [creative_height], [media_type], 
[fold_position], [event_type], [imp_type], [payment_type], [media_cost_dollars_cpm], [revenue_type], [buyer_spend], 
[buyer-bid], [ecp], [eap], [is_imp], [is_learn], [predict_type_rev], [user_id_64], [ip_address], [ip_address_trunc], 
[geo_country], [geo_region], [operating_system], [browser], [language], [venue_id], [seller_member_id], [publisher_id], 
[site_id], [site_domain], [tag_id], [external_inv_id], [reserve_price], [seller_revenue_cpm], 
[media_buy_rev_share_pct], [pub_rule_id], [seller_currency], [publisher_currency], [publisher_exchange_rate], 
[serving_fees_cpm], [serving_fees_revshare], [buyer_member_id], [advertiser_id], [brand_id], [advertiser_frequency], 
[advertiser_recency], [insertion_order_id], [campaign_group_id], [campaign_id], [creative_id], [creative_freq], 
[creative_rec], [cadence_modifier], [can_convert], [user_group_id], [is_control], [controller_pct], 
[controller_creative_pct], [is_click], [pixel_id], [is_remarketing], [post_click_conv], [post_view_conv], 
[post_click_revenue], [post_view_revenue], [order_id], [external_data], [pricing_type], [booked_revenue_dollars], 
[booked_revenue_adv_curr], [commission_cpm], [commission_revshare], [auction_service_deduction], 
[auction_service_fees], [creative_overage_fees], [clear_fees], [buyer_currency], [advertiser_currency], 
[advertiser_exchange_rate], [latitude], [longitude], [device_unique_id], [device_id], [carrier_id], [deal_id], 
[view_result], [application_id], [supply_type], [sdk_version], [ozone_id], [billing_period_id], 
[view_non_measurable_reason], [external_uid], [request_uuid], [geo_dma], [geo_city], [mobile_app_instance_id], 
[traffic_source_code], [external_request_id], [deal_type], [ym_floor_id], [ym_bias_id], [is_filtered_request], 
[age], [gender], [is_exclusive], [bid_priority], [custom_model_id], [custom_model_last_modified], [leaf_name], 
[data_costs_cpm], [device_type] 
FROM [apn].StandardFeedV2
UNION ALL 
SELECT [auction_id_64], [timestamp], [user_tz_offset], [creative_width], [creative_height], [media_type], 
[fold_position], [event_type], [imp_type], [payment_type], [media_cost_dollars_cpm], [revenue_type], [buyer_spend], 
[buyer-bid], [ecp], [eap], [is_imp], [is_learn], [predict_type_rev], [user_id_64], [ip_address], [ip_address_trunc], 
[geo_country], [geo_region], [operating_system], [browser], [language], [venue_id], [seller_member_id], [publisher_id], 
[site_id], [site_domain], [tag_id], [external_inv_id], [reserve_price], [seller_revenue_cpm], 
[media_buy_rev_share_pct], [pub_rule_id], [seller_currency], [publisher_currency], [publisher_exchange_rate], 
[serving_fees_cpm], [serving_fees_revshare], [buyer_member_id], [advertiser_id], [brand_id], [advertiser_frequency], 
[advertiser_recency], [insertion_order_id], [campaign_group_id], [campaign_id], [creative_id], [creative_freq], 
[creative_rec], [cadence_modifier], [can_convert], [user_group_id], [is_control], [controller_pct], 
[controller_creative_pct], [is_click], [pixel_id], [is_remarketing], [post_click_conv], [post_view_conv], 
[post_click_revenue], [post_view_revenue], [order_id], [external_data], [pricing_type], [booked_revenue_dollars], 
[booked_revenue_adv_curr], [commission_cpm], [commission_revshare], [auction_service_deduction], 
[auction_service_fees], [creative_overage_fees], [clear_fees], [buyer_currency], [advertiser_currency], 
[advertiser_exchange_rate], [latitude], [longitude], [device_unique_id], [device_id], [carrier_id], [deal_id], 
[view_result], [application_id], [supply_type], [sdk_version], [ozone_id], [billing_period_id], 
[view_non_measurable_reason], [external_uid], [request_uuid], [geo_dma], [geo_city], [mobile_app_instance_id], 
[traffic_source_code], [external_request_id], [deal_type], [ym_floor_id], [ym_bias_id], [is_filtered_request], 
[age], [gender], [is_exclusive], [bid_priority], [custom_model_id], [custom_model_last_modified], [leaf_name], 
[data_costs_cpm], [device_type] 
FROM [asb].StandardFeedV3;

```
