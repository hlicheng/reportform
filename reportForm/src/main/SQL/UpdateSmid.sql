INSERT OVERWRITE TABLE reporting.smid
(
SELECT t4.organization, t4.smid, t4.timestamp
FROM
(
SELECT t3.*,row_number() over (partition BY t3.organization, t3.smid ORDER BY t3.timestamp DESC ) as rank
FROM
(
SELECT t1.* FROM
(SELECT * FROM reporting.smid) AS t1
FULL JOIN
(SELECT * FROM smid_now) AS t2
ON t1.organization=t2.organization AND t1.smid=t2.smid
) t3
) t4
WHERE t4.rank=1
)