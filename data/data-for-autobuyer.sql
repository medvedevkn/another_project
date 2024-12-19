with matching as
(
	select campaign_id, adgroup_id
	from adwords_history
	where adgroup_id is not null
),
overall as 
(
select campaign_id, adgroup_id, change_date_time, event_type, null as old_value, json_extract(new_resource, '$.targetCpaMicros') as new_value
from adwords_history
where event_type = 'CREATE_AD_GROUP'
having new_value is not null
union
select campaign_id, adgroup_id, change_date_time, event_type, null as old_value, json_extract(new_resource, '$.status') as new_value
from adwords_history
where event_type = 'CREATE_AD_GROUP'
having new_value is not null
union
select campaign_id, adgroup_id, change_date_time, event_type, json_extract(old_resource, '$.status') as old_value,
	json_extract(new_resource, '$.status') as new_value
from adwords_history
where adgroup_id is not null
and event_type = 'UPDATE_AD_GROUP_STATUS'
union
select campaign_id, adgroup_id, change_date_time, event_type, json_extract(old_resource, '$.targetCpaMicros') as old_value,
	json_extract(new_resource, '$.targetCpaMicros') as new_value
from adwords_history
where event_type = 'UPDATE_AD_GROUP_TARGET_CPA'
having new_value is not null
union
select tt1.campaign_id, tt2.adgroup_id, change_date_time, event_type, json_extract(old_resource, '$.amountMicros') as old_value,
	json_extract(new_resource, '$.amountMicros') as new_value
from adwords_history tt1 
join matching tt2
on tt1.campaign_id = tt2.campaign_id
where event_type = 'UPDATE_CAMPAIGN_BUDGET_AMOUNT'
having new_value is not null
union
select tb1.campaign_id, tb2.adgroup_id, change_date_time, event_type, json_extract(old_resource, '$.adSchedule') as old_value,
	json_extract(new_resource, '$.adSchedule') as new_value
from adwords_history tb1
join matching tb2
on tb1.campaign_id = tb2.campaign_id
where event_type = 'CREATE_CAMPAIGN_CRITERION_AD_SCHEDULE'
having new_value is not null
union
select m.campaign_id, m.adgroup_id, change_date_time, event_type, json_extract(old_resource, '$.biddingStrategyType') as old_value,
	json_extract(new_resource, '$.biddingStrategyType') as new_value
from adwords_history ah
join matching m
on ah.campaign_id = m.campaign_id
where event_type = 'UPDATE_CAMPAIGN'
having new_value is not null
union 
select m.campaign_id, m.adgroup_id, change_date_time, event_type, json_extract(old_resource, '$.status') as old_value,
	json_extract(new_resource, '$.status') as new_value
from adwords_history ah
join matching m
on ah.campaign_id = m.campaign_id
where event_type = 'UPDATE_CAMPAIGN'
having new_value is not null
),
adg as 
(
select distinct adgroup_id from adwords_history
)

select distinct tb.*,
	case when tb.change_rate between 0 and 0.9999 then concat(tb.event_type, "_DOWN")
		 when tb.change_rate between 1.00001 and 10 then concat(tb.event_type, "_UP")
	end as cat
from
(
	select tbl.*, 
		case when tbl.event_type in ('UPDATE_AD_GROUP_TARGET_CPA', 'UPDATE_AD_GROUP_TARGET_CPA', 'UPDATE_CAMPAIGN_BUDGET', 'UPDATE_CAMPAIGN_BUDGET_AMOUNT') then tbl.new_value / tbl.old_value else null end as change_rate
	from
    (
		select t.date, t.adgroup_id, dense_rank() over (partition by t.adgroup_id order by t.date) as rn, t.impressions, t.bought, t.cost, t.sold, t.earned, t.cpa, t.rpc, t.profit,
            t.max_cpa, t.max_cpc, t.campaign_budget_amount, 
		t.change_date_time, case when t.campaign_budget_amount <> t.prev_budget then 'UPDATE_CAMPAIGN_BUDGET_AMOUNT' else t.event_type end as event_type,
			replace(case when t.campaign_budget_amount <> t.prev_budget then t.prev_budget else t.old_value end, '"', '') as old_value,
			replace(case when t.campaign_budget_amount <> t.prev_budget then t.campaign_budget_amount else t.new_value end, '"', '') as new_value,
            t.prior
		from
		(
		select date, t1.adgroup_id, impressions, bought, cost, sold, earned, cost / bought as cpa, sold / earned as rpc, earned - cost as profit, max_cpa, max_cpc, campaign_budget_amount, 
			t2.change_date_time, coalesce(t2.event_type, 'NO_CHANGE') as event_type, 
			case when t2.event_type in ('UPDATE_CAMPAIGN_BUDGET_AMOUNT','UPDATE_AD_GROUP_TARGET_CPA') then t2.old_value / 1000000 else t2.old_value end as old_value, 
			case when t2.event_type in ('UPDATE_CAMPAIGN_BUDGET_AMOUNT', 'UPDATE_AD_GROUP_TARGET_CPA') then t2.new_value / 1000000 else t2.new_value end as new_value,
			case when t2.event_type in ('UPDATE_AD_GROUP_TARGET_CPA', 'UPDATE_AD_GROUP_TARGET_CPA') then new_value / old_value end as change_rate,
			lag(t1.campaign_budget_amount, 1, t1.campaign_budget_amount) over (partition by t1.adgroup_id order by date) as prev_budget,
			lag(t2.event_type, 1, t2.event_type) over (partition by t1.adgroup_id order by date) as prev_event,
            case when event_type = 'UPDATE_CAMPAIGN' then 1 else 2 end as prior
		from adgroup_report_consolidated t1 
		left join overall t2 
		on t1.adgroup_id = t2.adgroup_id
		and t1.date = date(t2.change_date_time - interval 1 day)
		where t1.adgroup_id in (select distinct adgroup_id from adg)
		order by t1.adgroup_id, t1.date, t2.change_date_time
		) t
	) tbl
) tb
order by tb.adgroup_id, tb.date, tb.change_date_time, tb.prior