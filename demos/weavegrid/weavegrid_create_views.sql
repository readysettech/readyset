create view load_profiles as
  select lp.vehicle_id AS vehicle_id,
  util.utility_id AS utility_id,
  cast(convert_tz(lp.start_dttm,'UTC',util.time_zone) as date) AS local_date,
  cast(convert_tz(lp.start_dttm,'UTC',util.time_zone) as time) AS local_start_time,
  timediff(lp.end_dttm,lp.start_dttm) AS duration,
  sum(ifnull(lp.energy_delivered,lp.energy_added)) AS usage_kwh
  from ((((vehicle_load_profiles lp
  join vehicles v on(v.vehicle_id = lp.vehicle_id))
  join registrations r on(r.registration_id = v.registration_id))
  join users u on(u.user_id = r.user_id))
  join utilities util on(util.utility_id = u.utility_id))
  where lp.is_home = 1
  group by lp.vehicle_id, util.utility_id, lp.start_dttm, lp.end_dttm;

create view on_peak_usage as
  select
    lp.vehicle_id AS vehicle_id,
    lp.local_date AS local_date,
    sum(lp.usage_kwh) AS on_peak_usage_kwh
  from (((load_profiles lp
  join utility_peak_periods peak on lp.utility_id = peak.utility_id)
  join utilities util on (util.utility_id = peak.utility_id))
  left join (
       select h1.local_date AS local_date
       from (utility_holidays h1 join utilities u1 on(u1.utility_id = h1.utility_id))
  ) h on (h.local_date = lp.local_date))
  where h.local_date is null
  and month(lp.local_date) between peak.start_month and peak.end_month
  and dayofweek(lp.local_date) between 2 and 6
  and lp.local_start_time >= peak.local_start_time
  group by lp.vehicle_id,lp.local_date;

create view total_usage as
  select
  load_profiles.vehicle_id AS vehicle_id,
  load_profiles.utility_id AS utility_id,
  load_profiles.local_date AS local_date,
  sum(load_profiles.usage_kwh) AS total_usage_kwh
  from load_profiles
  group by load_profiles.vehicle_id, load_profiles.local_date;

create view vehicle_daily_usage as
    select
        t.vehicle_id AS vehicle_id,
        t.utility_id AS utility_id,
        t.local_date AS local_date,
        ifnull(p.on_peak_usage_kwh,cast(0.0 as double)) AS on_peak_usage_kwh,
        t.total_usage_kwh - ifnull(p.on_peak_usage_kwh,cast(0.0 as double)) AS off_peak_usage_kwh
    from (total_usage t
    left join on_peak_usage p
    on t.vehicle_id = p.vehicle_id and t.local_date = p.local_date);


