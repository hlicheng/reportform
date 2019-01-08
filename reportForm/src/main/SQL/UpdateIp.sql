INSERT overwrite table reporting.Ip (SELECT t4.organization,t4.ip,t4.timestamp
FROM
(SELECT t3.*,row_number() over (partition BY t3.organization, t1.ip ORDER BY t3.timestamp desc) as rank
FROM
(SELECT t1.* FROM
(SELECT * FROM reporting.Ip) t1
FULL JOIN
(SELECT * FROM ip_now) t2
ON t1.organization=t2.organization AND t1.ip=t2.ip) t3
) t4
WHERE t4.rank=1)