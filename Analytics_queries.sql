--Total Sessions in Each Year
SELECT Year, COUNT(*) AS total_sessions
FROM `ev-project-407007.Ev_project.charging_sessions_dim`
GROUP BY Year
ORDER BY Year;


--MONTHLY ANALYSIS:
SELECT Month, COUNT(*) AS total_sessions,
  AVG(Duration) AS avg_duration,
  SUM(Energy__kWh_) AS total_energy


FROM `ev-project-407007.Ev_project.charging_sessions_dim`
GROUP BY Month
ORDER BY total_sessions DESC;

--Hourly Analysis:
    --Top stations with most sessions in a hour of the day


SELECT s.station_name, c.Hour AS hour_of_day, COUNT(*) AS sessions
FROM `ev-project-407007.Ev_project.charging_sessions_dim` AS c
JOIN `ev-project-407007.Ev_project.stations_dim` AS s USING (station_id)
GROUP BY Station_Name, hour_of_day
ORDER BY sessions DESC ;
   --SELECT Hour AS hour_of_day

COUNT(*) AS total_sessions
   
FROM `ev-project-407007.Ev_project.charging_sessions_dim`
GROUP BY hour_of_day
ORDER BY hour_of_day;
--Analysis by Each season
SELECT
    season,
    COUNT(*) AS total_sessions,
    AVG(Duration) AS avg_duration,
    SUM(Energy__kWh_) AS total_energy,
   
FROM (
    SELECT
        *,
        CASE
            WHEN LOWER(MONTH) IN ('december', 'january', 'february') THEN 'Winter'
            WHEN LOWER(MONTH) IN ('march', 'april', 'may') THEN 'Spring'
            WHEN LOWER(MONTH) IN ('june', 'july', 'august') THEN 'Summer'
            WHEN LOWER(MONTH) IN ('september', 'october', 'november') THEN 'Autumn'
        END AS season
    FROM `ev-project-407007.Ev_project.charging_sessions_dim`
)
GROUP BY season
ORDER BY season

  



-- Percentage of consumption of each port_type
WITH total_sessions AS (
    SELECT COUNT(*) as total
    FROM `ev-project-407007.Ev_project.charging_sessions_dim`
),


port_sessions AS (
    SELECT
        port_type,
        COUNT(*) as num_sessions
    FROM
        `ev-project-407007.Ev_project.charging_sessions_dim`
    GROUP BY
        port_type
)


SELECT
    port_type,
    (num_sessions / (SELECT total FROM total_sessions)) * 100 as usage_percentage
FROM
    port_sessions

--  Percentage of repeated and non-repeated customers
WITH user_counts AS (
  SELECT
    User_ID,
    COUNT(*) AS num_sessions
  FROM
    `ev-project-407007.Ev_project.charging_sessions_dim`
  GROUP BY
    User_ID
),


repeat_users AS (
  SELECT
    COUNT(*) AS num_repeat_users
  FROM
    user_counts
  WHERE
    num_sessions > 1
),


non_repeat_users AS (
  SELECT
    COUNT(*) AS num_non_repeat_users
  FROM
    user_counts
  WHERE
    num_sessions = 1
),


total_users AS (
  SELECT
    COUNT(*) AS total_users
  FROM
    user_counts
)


SELECT
  (num_repeat_users / total_users.total_users) * 100 AS repeat_user_percentage,
  (num_non_repeat_users / total_users.total_users) * 100 AS non_repeat_user_percentage
FROM
  repeat_users,
  non_repeat_users,
  total_users


