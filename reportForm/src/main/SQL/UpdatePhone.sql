INSERT overwrite table reporting.tempphone
(SELECT t4.organization, t4.phone, t4.i_phone_last_active_time, t4.is_black
FROM
(
SELECT t3.*,row_number() over (partition BY t3.organization,phone ORDER BY t3.i_phone_last_active_time DESC ) as rank
FROM
(
SELECT t1.* FROM
(SELECT * FROM reporting.phone) t1
FULL JOIN
(SELECT * FROM phone_now) t2
ON t1.organization=t2.organization AND t1.phone=phone=t2.phone
) t3
) t4
where t4.rank=1)