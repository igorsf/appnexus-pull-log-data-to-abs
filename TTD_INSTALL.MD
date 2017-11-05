CREATE DATABASE SCOPED CREDENTIAL AzureTTDStorageCredential
WITH
    IDENTITY = 'user',
    SECRET = 'azure_storage_account_key>'
;

CREATE EXTERNAL FILE FORMAT RedsTabDelimitedFile
WITH
(
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS
    (
        FIELD_TERMINATOR = '\t',
        DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss.ffff',
        USE_TYPE_DEFAULT = FALSE
    ),
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.GzipCodec'
);


CREATE SCHEMA [ttd]
GO

CREATE EXTERNAL TABLE [ttd].ImpressionEvents (
    [log_entry_time] [nvarchar](20) NULL,
    [impression_id] [nvarchar](36) NULL,
    [partner_id] [nvarchar](32) NULL,
    [advertiser_id] [nvarchar](32) NULL,
    [campaign_id] [nvarchar](32) NULL,
    [ad_group_id] [nvarchar](32) NULL,
    [private_contract_id] [nvarchar](32) NULL,
    [audience_id] [nvarchar](32) NULL,
    [creative_id] [nvarchar](32) NOT NULL,
    [ad_format] [nvarchar](32) NULL,
    [frequency] [int] NULL,
    [supply_vendor] [nvarchar](32) NULL,
    [supply_vendor_publisher_id] [nvarchar](64) NULL,
    [deal_id] [nvarchar](32) NULL,
    [site] [nvarchar](512) NULL,
    [referrer_categories_list] [nvarchar] (4000) NULL,
    [fold_position] [tinyint] NULL,
    [user_hour_of_week] [tinyint] NULL,
    [user_agent] [nvarchar](4000) NULL,
    [ip_address] [nvarchar](128) NULL,
    [tdid] [nvarchar](36) NULL,
    [country] [nvarchar](64) NULL,
    [region] [nvarchar](128) NULL,
    [metro] [nvarchar](32) NULL,
    [city] [nvarchar](64) NULL,
    [device_type] [tinyint] NULL,
    [os_family] [tinyint] NULL,
    [os] [tinyint] NULL,
    [browser] [tinyint] NULL,
    [recency] [int] NULL,
    [language_code] [nvarchar](4) NULL,
    [media_cost] [numeric](18,12) NULL,
    [fee_feature_cost] [numeric](18,12) NULL,
    [data_usage_total_cost] [numeric](18,12) NULL,
    [ttd_cost_usd] [numeric](18,12) NULL,
    [partner_cost_usd] [numeric](18,12) NULL,
    [advertiser_cost_usd] [numeric](18,12) NULL,
    [latitude] [float] NULL,
    [longtitude] [float] NULL,
    [device_id] [nvarchar](36) NULL,
    [zipcode] [nvarchar](20) NULL,
    [processed_time] [datetime2](4) NULL,
    [device_make] [nvarchar](128) NULL,
    [device_model] [nvarchar](128) NULL,
    [rendering] [nvarchar](32) NULL,
    [carrier_id] [nvarchar](32) NULL,
    [temp_in_celcius] [numeric](3) NULL,
    [temp_bucket_start_in_celcius] [int] NULL,
    [temp_bucket_end_in_celcius] [int] NULL
)
WITH
(
    LOCATION='/impressions/'
,   DATA_SOURCE = AzureRedsStorage
,   FILE_FORMAT = RedsTabDelimitedFile
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
)
;

CREATE EXTERNAL TABLE [ttd].ClickEvents (
    [log_entry_time] [nvarchar](20) NULL,
    [click_id] [nvarchar](36) NULL,
    [ip_address] [nvarchar](128) NULL,
    [referrer_url] [nvarchar] (512) NULL,
    [campaign_id] [nvarchar](32) NULL,
    [channel_id] [nvarchar](32) NULL,
    [advertiser_id] [nvarchar](32) NULL,
    [display_impression_id] [nvarchar](36) NULL,
    [keyword] [nvarchar] (512) NOT NULL,
    [keyword_id] [nvarchar](16) NULL,
    [match_type] [nvarchar] (16) NULL,
    [distribution_network] [nvarchar](255) NULL,
    [tdid] [nvarchar](36) NULL,
    [raw_url] [nvarchar] (512) NULL,
    [processed_time] [datetime2](4) NULL,
    [device_id] [nvarchar](36) NULL
)
WITH
(
    LOCATION='/clicks/'
,   DATA_SOURCE = AzureRedsStorage
,   FILE_FORMAT = RedsTabDelimitedFile
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
)
;

CREATE EXTERNAL TABLE [ttd].VideoEvents (
    [log_entry_time] [nvarchar](20) NULL,
    [impression_id] [nvarchar](36) NULL,
    [video_event_creative_view] [tinyint] NULL,
    [video_event_start] [tinyint] NULL,
    [video_event_first_quarter] [tinyint] NULL,
    [video_event_midpoint] [tinyint] NULL,
    [video_event_third_quarter] [tinyint] NULL,
    [video_event_complete] [tinyint] NULL,
    [video_event_muted] [tinyint] NOT NULL,
    [video_event_unmuted] [tinyint] NULL,
    [creatives_is_trackable] [nvarchar] (6) NULL,
    [creatives_was_viewable] [nvarchar](6) NULL,
    [video_play_time_in_seconds] [int] NULL,
    [video_viewable_time_in_seconds] [int] NULL,
    [video_event_companion_creative_view] [tinyint] NULL,
    [processed_time] [datetime2](4) NULL
)
WITH
(
    LOCATION='/videoevents/'
,   DATA_SOURCE = AzureRedsStorage
,   FILE_FORMAT = RedsTabDelimitedFile
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
)
;


CREATE EXTERNAL TABLE [ttd].Conversions (
    [log_entry_time] [nvarchar](20) NULL,
    [conversion_id] [nvarchar](36) NULL,
    [advertiser_id] [nvarchar](32) NULL,
    [conversion_type] [nvarchar](32) NULL,
    [tdid] [nvarchar](36) NULL,
    [ip_address] [nvarchar](128) NULL,
    [referrer_url] [nvarchar] (512) NULL,
    [monetary] [numeric](18,6) NULL,
    [field] [nvarchar] (255) NOT NULL,
    [monetary_value_currency] [nvarchar](4) NULL,
    [order_id] [nvarchar] (255) NULL,
    [td1] [nvarchar](255) NULL,
    [td2] [nvarchar](255) NULL,
    [td3] [nvarchar](255) NULL,
    [td4] [nvarchar](255) NULL,
    [td5] [nvarchar](255) NULL,
    [td6] [nvarchar](255) NULL,
    [td7] [nvarchar](255) NULL,
    [td8] [nvarchar](255) NULL,
    [td9] [nvarchar](255) NULL,
    [processed_time] [datetime2](4) NULL
)
WITH
(
    LOCATION='/conversions/'
,   DATA_SOURCE = AzureRedsStorage
,   FILE_FORMAT = TabDelimitedFile
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
)
;