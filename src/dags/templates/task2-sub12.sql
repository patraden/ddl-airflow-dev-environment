/* 
 * Для каждого пользователя число таких сделок, 
 * у которых с момента открытия до момента полного закрытия прошло меньше одной минуты.
 * 
 * Для каждого пользователя найти число таких пар сделок, которые удовлетворяют следующим условиям:
 * a. Сделки совершены этим пользователем
 * b. Разница между временем их открытия не более 30 секунд
 * c. Направление этих сделок – противоположное (одна на покупку, другая на продажу) 
 * */

-- In this query we conciously do not union trades from mt4 and mt5 as logins do not intersect.
-- Rather we calc both metrics for each and then union
-- Also both metrics could have been calced in single join (1st metric through dictinct count), however this seems to be slower query.

with trades_filtered as not materialized
(
select t.* 
from mt4.trades t
left join mt4.marked_trades mt
	on t.ticket = mt.ticket and mt."type" % 2 = 1
where mt.ticket is null
), deals_pre_filtered as not materialized
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
), trades_within_a_minute as 
(
select 
	login, 
	count(case when close_time - open_time between '00:00:00.000' and '00:00:59.999' then ticket else null end) qty
from trades_filtered
group by login
), deals_within_a_minute as 
(
select 
	login, 
	count(case when close_time - open_time between '00:00:00.000' and '00:00:59.999' then ticket else null end) qty
from deals_filtered
group by login
), trades_pairs as 
(
select
	t1.login,
	count(t2.ticket) qty
from trades_filtered t1
left join trades_filtered t2 
	on t1.login = t2.login and
	t1.ticket <> t2.ticket and
	t1.cmd <> t2.cmd and
	t1.open_time - t2.open_time between '00:00:00.000' and '00:00:29.999' -- avoiding duplicate pairs
group by t1.login
), deals_pairs as 
(
select
	t1.login,
	count(t2.ticket) qty
from deals_filtered t1
left join deals_filtered t2 
	on t1.login = t2.login and
	t1.ticket <> t2.ticket and
	t1.cmd <> t2.cmd and
	t1.open_time - t2.open_time between '00:00:00.000' and '00:00:29.999' -- avoiding duplicate pairs
group by t1.login
)
select tm.login usr, tm.qty qty_quick_trades, tp.qty qty_trade_pairs
from trades_within_a_minute tm
join trades_pairs tp
	on tm.login = tp.login
union all
select tm.login, tm.qty, tp.qty
from deals_within_a_minute tm
join deals_pairs tp
	on tm.login = tp.login