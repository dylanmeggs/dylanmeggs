-- Preliminary analysis to review signal integrity
-- Draw insights into signal issues
-- Which vehicles and what type of impact does each have

DROP TABLE IF EXISTS vehicle_data;
CREATE TEMP TABLE vehicle_data AS
    SELECT vin
        , make
        , model
    FROM team_prod.dim_vehicle_info
    GROUP BY 1
;

DROP TABLE IF EXISTS overall_values;
CREATE TEMP TABLE overall_values AS
SELECT
        'All Makes' as make
    ,   'All Models' as model
    ,   count(distinct sd.vin || sd.route_id) as total_routes_tt
    ,   count(distinct case when (sd2.vin is null and sd2.route_id is null) then sd.vin || sd.route_id end) as perfect_routestt
    ,   count(distinct case when (sd2.vin is not null and sd2.route_id is not null) then sd.vin || sd.route_id end) as imperfect_routestt
    ,   count(distinct case when (sd.signal_status = 'Option A') then sd.vin || sd.route_id end) as opt_a_routestt
    ,   count(distinct case when (sd.signal_status = 'Data Not Available') then sd.vin || sd.route_id end) as not_avail_routestt

FROM team_stage.stg_signal_details sd
LEFT JOIN (
        SELECT
              vin
            , route_id
            , count(*) as total
        FROM team_stage.stg_signal_details
        WHERE signal_status IN ('Data Not Available', 'Option A')
        AND data_dt between '2023-12-01' and '2023-12-31'
        GROUP BY 1,2
        ) sd2
on sd.vin = sd2.vin
and sd.route_id = sd2.route_id
JOIN vehicle_data vd
ON sd.vin = vd.vin
where sd.data_dt between '2023-12-01' and '2023-12-31'
group by 1,2;


SELECT
        vd.max_make as make
    ,   vd.max_model as model
    ,   count(distinct sd.vin || sd.route_id) as total_routes
    ,   count(distinct case when (sd2.vin is null and sd2.route_id is null) then sd.vin || sd.route_id end) as perfect_routes
    ,   count(distinct case when (sd2.vin is not null and sd2.route_id is not null) then sd.vin || sd.route_id end) as imperfect_routes
    ,   count(distinct case when (sd.signal_status = 'Option A') then sd.vin || sd.route_id end) as opt_a_routes
    ,   count(distinct case when (sd.signal_status = 'Data Not Available') then sd.vin || sd.route_id end) as not_avail_routes
    -- The three below tell you distribution across each make and model. They'll add up to 100.
    ,   (1.0 * perfect_routes / total_routes) as perfect_perc_make_model
    ,   (1.0 * opt_a_routes / total_routes) as open_perc_make_model
    ,   (1.0 * not_avail_routes / total_routes) as na_perc_make_model

    -- These 3 compare signal category values to network total (overall)
    ,   (1.0 * perfect_routes / ov.total_routes_tt) as perfect_perc_total
    ,   (1.0 * opt_a_routes / ov.total_routes_tt) as open_perc_total
    ,   (1.0 * not_avail_routes / ov.total_routes_tt) as na_perc_total

    -- The three below give the percent of this grouping vs network total for that signal category
    -- These are easier to interpret in Excel charts
    ,   (1.0 * perfect_routes / ov.perfect_routestt) as perf_perc_perf
    ,   (1.0 * opt_a_routes / ov.opt_a_routestt) as open_perc_open
    ,   (1.0 * not_avail_routes / ov.not_avail_routestt) as na_perc_na
FROM team_stage.stg_signal_details sd
LEFT JOIN (
        SELECT
              vin
            , route_id
            , count(*) as total
        FROM team_stage.stg_signal_details
        WHERE signal_status IN ('Data Not Available', 'Option A')
        AND data_dt between '2023-12-01' and '2023-12-31'
        GROUP BY 1,2
        ) sd2
on sd.vin = sd2.vin
and sd.route_id = sd2.route_id
JOIN vehicle_data vd
ON sd.vin = vd.vin
CROSS JOIN overall_values ov
where sd.data_dt between '2023-12-01' and '2023-12-31'
group by 1,2, ov.total_routes_tt, ov.perfect_routestt, ov.opt_a_routestt, ov.not_avail_routestt


union all

--Overall
SELECT
        make
    ,   model
    ,   total_routes_tt as total_routes
    ,   perfect_routestt as perfect_routes
    ,   imperfect_routestt as imperfect_routes
    ,   opt_a_routestt as opt_a_routes
    ,   not_avail_routestt as not_avail_routes
    ,   (1.0 * 100 / 100) as perfect_perc_make_model
    ,   (1.0 * 100 / 100) as open_perc_make_model
    ,   (1.0 * 100 / 100) as na_perc_make_model
    ,   (1.0 * perfect_routestt / total_routes_tt) as perfect_perc_total
    ,   (1.0 * opt_a_routestt / total_routes_tt) as open_perc_total
    ,   (1.0 * not_avail_routestt / total_routes_tt) as na_perc_total
    ,   (1.0 * perfect_routestt / perfect_routestt) as perf_perc_perf
    ,   (1.0 * opt_a_routestt / opt_a_routestt) as open_perc_open
    ,   (1.0 * not_avail_routestt / not_avail_routestt) as na_perc_na
FROM overall_values
order by 1,2;