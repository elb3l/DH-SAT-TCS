# DH SAE | Technical Test case

I would like to start by saying I would have liked more context of the business model this case study represents and a data dictionary would have been awesome.

Also, without a question a stakeholder engagement is necessary to get the most out this case study.   
   

## ER Diagram

https://dbdiagram.io/d/Delivery-Hero-Staff-Analytics-Engineering-or-Technical-Test-case-67465bb2e9daa85acad130b6

## Code
<br />

**`qualified_customer_orders` materialized view**

The qualified_customer_orders materialized view provides a consolidated view of customer orders enriched with the latest customer contact details for each order. It includes information on the order's success or failure status, monetary details such as order value and discounts, and the most recent customer interaction date and reason. The table ensures uniqueness at the order_id level and supports advanced analytics on customer engagement and order outcomes.

Key Features:
  * Uniqueness guaranteed by order_id.
  * Latest customer contact date (last_contact_date) and reason (last_contact_reason) and no of contacts.
  * Comprehensive order metadata (brand, customer ID, value, status).

<br />

```sql

create materialized view dh-codapro-analytics-2460.hiring_search_analytics.qualified_customer_orders
as
with customer_orders_data as 
  (
    select 
      partition_date
    , placed_at_utc
    , order_id
    , brand
    , customer_id
    , order_value_eur
    , discount_value_eur
    , is_successful
    , voucher_value_eur
    , c.contact_at
    , c.reason
    , count(*) over(partition by order_id) as no_of_cs_contact 
    , row_number() over(partition by order_id order by case when c.reason = 'Post-Delivery' then 'AAAAA' else c.reason end) as rnk 
    from `dh-codapro-analytics-2460.hiring_search_analytics.customer_orders_data` , unnest(customer_contact) c
  )
select
  partition_date
, placed_at_utc
, order_id
, brand
, customer_id
, order_value_eur
, discount_value_eur
, is_successful
, voucher_value_eur
, no_of_cs_contact
, contact_at as last_contact_at
, reason as last_contact_reason
from customer_orders_data
where true
and rnk = 1
;
```
<br />

**`experiments` materialized view**

The experiments materialized view consolidates experiment-related data from the backend_logging_data table to support detailed analysis of user experiments. It extracts critical fields such as session, client IDs, and experiment details while cleaning and parsing the JSON payload for relevant attributes. The view allows analysts to efficiently track, query, and assess the impact of experiments across various dimensions like country, brand, and user types.

Key Points:
  * Experiment Tracking: Captures experiment identifiers, variations, and AB test status.
  * Session and Client: Tracks user activity using session_id and client_id.
  * Payload Cleaning: Removes the vendors field using JSON_REMOVE to simplify data structure.
    * this how we foucsed on relevent attributes for later extraction.
  * Field Parsing: Extracts important fields from `Payload` such as:
    * Country Code
    * Language Code
    * Brand (not sure why it's different from higher level Brand field!!)
    * Pro-customer flag

Purpose: Facilitates analysis of experiment performance by aggregating key metrics and dimensions.

<br />

```sql

create materialized view dh-codapro-analytics-2460.hiring_search_analytics.experiments
as
with experiments as
    (
        select
          partition_date
        , brand
        , perseus_session_id as session_id
        , perseus_id as client_id
        , global_entity_id
        , request_id
        , response_id
        , fun_with_flags_client.response.`experiments`[SAFE_OFFSET(0)].key as experiment_key
        , fun_with_flags_client.response.`experiments`[SAFE_OFFSET(0)].variation as experiment_variation
        , fun_with_flags_client.response.`experiments`[SAFE_OFFSET(0)].variation_name as experiment_variation_name
        , fun_with_flags_client.response.`experiments`[SAFE_OFFSET(0)].abtest as experiment_abtest_flg
        , JSON_REMOVE(PARSE_JSON(payload), '$.vendors') as payload_no_vendors
        FROM `dh-codapro-analytics-2460.hiring_search_analytics.backend_logging_data` 
    )
    select
      partition_date
    , brand
    , session_id
    , client_id
    , global_entity_id
    , request_id
    , response_id
    , experiment_key
    , experiment_variation
    , experiment_variation_name
    , experiment_abtest_flg
    , JSON_VALUE(payload_no_vendors.country_code) as country_code
    , JSON_VALUE(payload_no_vendors.language_code) as language_code
    , JSON_VALUE(payload_no_vendors.brand) as payload_brand
    -- , JSON_VALUE(payload_no_vendors.config) as payload_config --same as experiment_variation
    , JSON_VALUE(payload_no_vendors.pro_customer) as pro_customer
    -- , (JSON_KEYS(payload_no_vendors, mode => "lax recursive")) as list_of_payload_keys
    from experiments
    ;
```
<br />

**`events` materialized view**

The events materialized view aggregates customer interaction data to facilitate tracking of user journeys, event progression, and transaction-related insights. It enhances event-level data with ordered classifications and aggregates event names to derive key insights, such as the furthest event reached and the total number of events per session.

Key Features:
  * List of Events (`list_of_events`): Ordered list of unique events in each session, reflecting the user's journey.
    * Assigns a numerical prefix (01., 02., etc.) to important events like `app.open`, `add_cart.click`, and `transaction` to facilitate ordering and analysis.
    * `ARRAY_AGG` to generate lists of distinct events, ordered by significance, and captures the maximum event reached.
  * Max Event Reached (`max_event_reached`): The last event reached in the session, useful for conversion funnel analysis.
  * Number of Events (`no_of_events`): Total count of events within a session, indicating engagement levels.
  * Designed to support analysis of user behaviors, session-level engagement, and event-based metrics, contributing to micro-conversion analysis and experiment tracking.

It's also worth mentioning that there are full record duplications `in behavioural_customer_data` `121696` vs. `121451` unique record.

<br />

```sql

create materialized view dh-codapro-analytics-2460.hiring_search_analytics.events
as
with behavioural_customer_data as
  (
    select
      partition_date
    , brand
    , device
    , session_id
    , client_id
    , customer_id
    , event_id
    , event_name
    , transaction_id
    , global_entity_id
    , case 
        when event_name = 'app.open' then '01.'||event_name
        when event_name = 'add_cart.click' then '02.'||event_name
        when event_name = 'experiment.participated' then '03.'||event_name
        when event_name = 'shop_list.loaded' then '04.'||event_name
        when event_name = 'transaction' then '05.'||event_name else event_name end as ordered_event_name
    -- , max(case when var.name in ('experimentId') then var.value end) as experimentId
    -- , max(case when var.name in ('experimentVariation') then var.value end) as experimentVariation
    from `dh-codapro-analytics-2460.hiring_search_analytics.behavioural_customer_data` , unnest(event_variables) var 
    group by 1,2,3,4,5,6,7,8,9,10,11
  )
  select
    partition_date
  , brand
  , device
  , session_id
  , client_id
  , customer_id
  , array_agg(distinct ordered_event_name order by ordered_event_name desc) as list_of_events
  , array_agg(distinct ordered_event_name order by ordered_event_name desc)[SAFE_OFFSET(0)] as max_event_reached
  , max(transaction_id) as transaction_id
  , global_entity_id
  -- , experimentId
  -- , experimentVariation
  , count(*) as no_of_events
  FROM behavioural_customer_data
  where true
  group by 
    partition_date
  , brand
  , device
  , session_id
  , client_id
  , customer_id
  , global_entity_id
  -- , experimentId
  -- , experimentVariation  
  ;
  
```
<br />

**Query 1: `assignments` materialized view**

The assignments materialized view integrates customer order data, behavioral events, and experimental results, enabling comprehensive analysis of customer interactions, order details, and experiment participation. This view helps track customer engagement, purchase behaviors, and experiment outcomes across various dimensions such as regions, devices, and session identifiers.

Key Features:
  * Merges order, event, and experiment data based on session and customer identifiers. 
  * Provides a consolidated view with key attributes: order success, device, experiment participation, and customer details.

<br />

```sql

create materialized view dh-codapro-analytics-2460.hiring_search_analytics.assignments
as
with customer_orders_data as 
  (
    select 
      partition_date
    , placed_at_utc
    , order_id
    , brand
    , customer_id
    , order_value_eur
    , discount_value_eur
    , is_successful
    , voucher_value_eur
    , c.contact_at
    , c.reason
    , count(*) over(partition by order_id) as no_of_cs_contact 
    , row_number() over(partition by order_id order by case when c.reason = 'Post-Delivery' then 'AAAAA' else c.reason end) as rnk 
    from `dh-codapro-analytics-2460.hiring_search_analytics.customer_orders_data` , unnest(customer_contact) c
  )
  , qualified_customer_orders as 
  (
    select
      partition_date
    , placed_at_utc
    , order_id
    , brand
    , customer_id
    , order_value_eur
    , discount_value_eur
    , is_successful
    , voucher_value_eur
    , no_of_cs_contact
    , contact_at as last_contact_at
    , reason as last_contact_reason
    from customer_orders_data
    where true
    and rnk = 1
  )
, behavioural_customer_data as
  (
    select
      partition_date
    , brand
    , device
    , session_id
    , client_id
    , customer_id
    , event_id
    , event_name
    , transaction_id
    , global_entity_id
    , case 
        when event_name = 'app.open' then '01.'||event_name
        when event_name = 'add_cart.click' then '02.'||event_name
        when event_name = 'experiment.participated' then '03.'||event_name
        when event_name = 'shop_list.loaded' then '04.'||event_name
        when event_name = 'transaction' then '05.'||event_name else event_name end as ordered_event_name
    -- , max(case when var.name in ('experimentId') then var.value end) as experimentId
    -- , max(case when var.name in ('experimentVariation') then var.value end) as experimentVariation
    from `dh-codapro-analytics-2460.hiring_search_analytics.behavioural_customer_data` , unnest(event_variables) var 
    group by 1,2,3,4,5,6,7,8,9,10,11
  )
  ,  events as (
  select
    partition_date
  , brand
  , device
  , session_id
  , client_id
  , customer_id
  , array_agg(distinct ordered_event_name order by ordered_event_name desc) as list_of_events
  , array_agg(distinct ordered_event_name order by ordered_event_name desc)[SAFE_OFFSET(0)] as max_event_reached
  , max(transaction_id) as transaction_id
  , global_entity_id
  -- , experimentId
  -- , experimentVariation
  , count(*) as no_of_events
  FROM behavioural_customer_data
  where true
  group by 
    partition_date
  , brand
  , device
  , session_id
  , client_id
  , customer_id
  , global_entity_id
  -- , experimentId
  -- , experimentVariation
  ) 
  ,  experiments_unnested as
    (
        select
          partition_date
        , brand
        , perseus_session_id as session_id
        , perseus_id as client_id
        , global_entity_id
        , request_id
        , response_id
        , fun_with_flags_client.response.`experiments`[SAFE_OFFSET(0)].key as experiment_key
        , fun_with_flags_client.response.`experiments`[SAFE_OFFSET(0)].variation as experiment_variation
        , fun_with_flags_client.response.`experiments`[SAFE_OFFSET(0)].variation_name as experiment_variation_name
        , fun_with_flags_client.response.`experiments`[SAFE_OFFSET(0)].abtest as experiment_abtest_flg
        , JSON_REMOVE(PARSE_JSON(payload), '$.vendors') as payload_no_vendors
        FROM `dh-codapro-analytics-2460.hiring_search_analytics.backend_logging_data` 
    )
    , experiments as (
    select
      partition_date
    , brand
    , session_id
    , client_id
    , global_entity_id
    , request_id
    , response_id
    , experiment_key
    , experiment_variation
    , experiment_variation_name
    , experiment_abtest_flg
    , JSON_VALUE(payload_no_vendors.country_code) as country_code
    , JSON_VALUE(payload_no_vendors.language_code) as language_code
    , JSON_VALUE(payload_no_vendors.brand) as payload_brand
    -- , JSON_VALUE(payload_no_vendors.config) as payload_config --same as experiment_variation
    , JSON_VALUE(payload_no_vendors.pro_customer) as pro_customer
    -- , (JSON_KEYS(payload_no_vendors, mode => "lax recursive")) as list_of_payload_keys
    from experiments_unnested
    )
    , assignments as (
    select
        o.partition_date
      , o.placed_at_utc
      , o.order_id
      , o.brand
      , o.customer_id
      , o.order_value_eur
      , o.discount_value_eur
      , o.is_successful
      , o.voucher_value_eur
      , o.no_of_cs_contact
      , o.last_contact_at
      , o.last_contact_reason
      , e.device
      , e.session_id
      , e.client_id      
      , e.list_of_events
      , e.max_event_reached
      , e.transaction_id
      , e.global_entity_id
      -- , e.experimentId
      -- , e.experimentVariation
      , e.no_of_events
      , x.request_id
      , x.response_id
      , x.experiment_key
      , x.experiment_variation
      , x.experiment_variation_name
      , x.experiment_abtest_flg
      , x.country_code
      , x.language_code
      , x.payload_brand
      , x.pro_customer
    from qualified_customer_orders o
    left join events e on o.partition_date = e.partition_date and o.customer_id = e.customer_id and o.order_id = e.transaction_id
    left join experiments x on e.partition_date = x.partition_date and e.session_id = x.session_id and e.client_id = x.client_id
    )
    select 
    *
    from assignments;
  
```
<br />

**Query 2: Show the conversion rate for each variant of an experiment, broken down by country and device type.**

<br />

```sql

    select
      experiment_key
    , experiment_variation
    , experiment_variation_name     
    , country_code
    , device
    , is_successful
    , count(distinct order_id) as no_of_orders
    from assignments
    group by 1,2,3,4,5,6;
```    
<br />

**Query 3: Show the Order value without vouchers and discounts for each variant of an experiment, aggregated by session identifier and broken down by region and device type.**

<br />

```sql

    select
      session_id
    , experiment_variation
    , country_code
    , device
    , sum(COALESCE(order_value_eur, 0) - COALESCE(discount_value_eur, 0) - COALESCE(voucher_value_eur, 0)) as net_order_value
    from assignments
    where is_successful is true
    group by 1,2,3,4;
``` 

<br />
</details>