/**
 * Мысленно разобьем все время на равные промежутки по 30 секунд.
 * Найти среди всех возможных пар пользователей такие, у которых более 10 сделок удовлетворяют условиям:
 * a. Открытия сделок попали в один промежуток времени
 * b. Они открыты по одному и тому же инструменту
 * c. Они принадлежат разным пользователям (одна одномупользователю из пары, другая другому)
 * d. Они открыты в разных направлениях (одна на покупку, другая на продажу)
 */

with trades_all as not materialized
(
-- all trades filtered from both mt4 and mt4 schemas
select 
	t.ticket,
	t.login,
	t.symbol,
	t.cmd,
	case when t.open_time - DATE_TRUNC('minute', t.open_time) <= '00:00:30.000' 
		then DATE_TRUNC('minute', t.open_time) 
		else DATE_TRUNC('minute', t.open_time) + '00:00:30.000'
	end open_time_interval
from mt4.trades t
left join mt4.marked_trades mt
	on t.ticket = mt.ticket and mt."type" % 2 = 1
where mt.ticket is null
union all
select 
	d.positionid ticket,
	d.login,
	d.symbol,
	d."action" cmd,
	case when d."time" - DATE_TRUNC('minute', d."time") <= '00:00:30.000' 
		then DATE_TRUNC('minute', d."time") 
		else DATE_TRUNC('minute', d."time") + '00:00:30.000'
	end open_time_interval
from mt5.deals d
left join mt5.marked_trades mt
	on d.positionid = mt.positionid and mt."type" % 2 = 1
where mt.positionid is null and d.entry = 0
)
select t1.login usr1, t2.login usr2
from trades_all t1
join trades_all t2
	on t1.login < t2.login and -- avoiding duplicate pairs
	t1.open_time_interval = t2.open_time_interval and
	t1.symbol = t2.symbol and
	t1.cmd <> t2.cmd
group by t1.login, t2.login
having count(*) > 10