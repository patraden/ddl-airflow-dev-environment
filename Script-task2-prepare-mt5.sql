with deals_pre_filtered as not materialized
(
select 
	d.*, 
	max(d."time") over (partition by d.positionid) time_max
from mt5.deals d
left join mt5.marked_trades mt
	on d.positionid = mt.positionid and mt."type" % 2 = 1
where mt.positionid is null
), deals_filtered as not materialized
(
select 
	positionid ticket,
	login,
	"time" open_time,
	case when time_max = "time" then null else time_max end close_time,
	symbol,
	"action" cmd
from deals_pre_filtered
where entry = 0
)
select count(*) from deals_filtered