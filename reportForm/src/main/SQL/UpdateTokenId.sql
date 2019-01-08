INSERT OVERWRITE table reporting.tokenid
(
select t4.organization, t4.tokenid, t4.i_token_last_active_time, t4.is_black
from
(
select t3.*, row_number() over (partition by t3.organization, t3.tokenid order by t3.i_token_last_active_time desc) as rank
from
(
select t1.* from
(select * from reporting.tokenid) as t1
full join
(select * from token_now) as t2
on t1.organization=t2.organization and t1.tokenid=t2.tokenid
) t3
) t4
where t4.rank=1
)