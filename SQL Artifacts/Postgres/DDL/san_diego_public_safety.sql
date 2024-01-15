CREATE SCHEMA stage;

CREATE TABLE stage.d_san_diego_collision_events

(
  report_id text
, date_time timestamp
, person_role text
, person_injury_lvl text
, person_veh_type text
, veh_type text
, veh_make text
, veh_model text
, police_beat int
, address_no_primary int 
, address_pd_primary text
, address_road_primary text
, address_sfx_primary text
, address_pd_intersecting text
, address_name_intersecting text
, address_sfx_intersecting text
, violation_section text
, violation_type text
, charge_desc text
, injured int
, killed int
, hit_run_lvl text
);



CREATE SCHEMA dmeggs_prod;

--DROP TABLE dmeggs_prod.d_san_diego_collision_events;
CREATE TABLE dmeggs_prod.d_san_diego_collision_events

(
  report_id text
, event_timestamp_pt timestamp
, event_date date
, person_role text
, person_injury_lvl text
, person_veh_type text
, veh_type text
, veh_make text
, veh_model text
, police_beat int
, address_no_primary int 
, address_pd_primary text
, address_road_primary text
, address_sfx_primary text
, address_pd_intersecting text
, address_name_intersecting text
, address_sfx_intersecting text
, violation_section text
, violation_type text
, charge_desc text
, injured int
, killed int
, hit_run_lvl text
, dataset_update_timestamp timestamp
);

\copy dmeggs_prod.d_san_diego_collision_events from '</Users/dylanmeggs/Downloads/pd_collisions_details_datasd.csv>' delimiter ',' CSV HEADER;


/*
ERROR:  could not open file "</Users/dylanmeggs/Downloads/pd_collisions_details_datasd.csv>" for reading: No such file or directory
HINT:  COPY FROM instructs the PostgreSQL server process to read a file. You may want a client-side facility such as psql's \copy. 

SQL state: 58P01

-- psql won't launch in my terminal. I tried 3 methods of achieving this and reinstalled postgres twice.
-- Stack overflow said I needed to update the file sharing/permissions to give everyone read and write perms.
-- This didn't work
-- I wound up using the pgmanager UI to import file directly to table

*/

INSERT INTO dmeggs_prod.d_san_diego_collision_events 
SELECT 
  nullif(trim(upper(report_id)),'')                 as report_id
, nullif(trim(upper(date_time)),'')                 as event_timestamp_pt
, nullif(trim(upper(date_time::date)),'')           as event_date
, nullif(trim(upper(person_role)),'')               as person_role
, nullif(trim(upper(person_injury_lvl)),'')         as person_injury_lvl
, nullif(trim(upper(veh_type)),'')                  as veh_type
, nullif(trim(upper(veh_make)),'')                  as veh_make
, nullif(trim(upper(veh_model)),'')                 as veh_model
, nullif(trim(upper(police_beat)),'')               as police_beat
, nullif(trim(upper(address_no_primary)),'')        as address_no_primary
, nullif(trim(upper(address_pd_primary)),'')        as address_pd_primary
, nullif(trim(upper(address_road_primary)),'')      as address_road_primary
, nullif(trim(upper(address_sfx_primary)),'')       as address_sfx_primary
, nullif(trim(upper(address_pd_intersecting)),'')   as address_pd_intersecting
, nullif(trim(upper(address_name_intersecting)),'') as address_name_intersecting
, nullif(trim(upper(address_sfx_intersecting)),'')  as address_sfx_intersecting
, nullif(trim(upper(violation_section)),'')         as violation_section
, nullif(trim(upper(violation_type)),'')            as violation_type
, nullif(trim(upper(charge_desc)),'')               as charge_desc
, nullif(trim(upper(injured)),'')                   as injured
, nullif(trim(upper(killed)),'')                    as killed
, nullif(trim(upper(hit_run_lvl)),'')               as hit_run_lvl
, now()                                             as dataset_update_timestamp
FROM stage.d_san_diego_collision_events
;




-----------------------------------------------------------------------------------------------------------------------------
ripa_stop_result_datasd

  stop_id
, pid
, resultkey
, result
, code
, resulttext



CREATE TABLE stage.d_stop_result_datasd

(
  stop_id int
, pid int
, resultkey int
, result text
, code int
, resulttext text
);



--DROP TABLE dmeggs_prod.d_stop_result_datasd;
CREATE TABLE dmeggs_prod.d_stop_result_datasd

(
  stop_id int
, pid int
, resultkey int
, result text
, code int
, resulttext text
, dataset_update_timestamp timestamp
);



INSERT INTO dmeggs_prod.d_stop_result_datasd 
SELECT 
  stop_id 
, pid 
, resultkey 
, result 
, code 
, resulttext 
, now()                 as dataset_update_timestamp
FROM stage.d_stop_result_datasd
;



ripa_stops_ds 

CREATE TABLE stage.d_stop_events_sd

(
stop_id int
, ori text
, agency text
, exp_years int
, date_stop date
, time_stop text --time only
, stopduration int
, stop_in_response_to_cfs int
, officer_assignment_key int
, assignment text
, intersection text
, address_block text
, land_mark text
, address_street text
, highway_exit text
, isschool bool
, school_name text
, address_city text
, beat text
, beat_name text
, pid int
, isstudent bool
, perceived_limited_english bool
, perceived_age int
, perceived_gender text
, gender_nonconforming bool
, gend int
, gend_nc text
, perceived_lgbt text
);

-- Storing raw stage data as text format to prevent future load errors and improve ease of transformations
-- DROP TABLE 
CREATE TABLE stage.d_stop_events_sd

(
stop_id text
, ori text
, agency text
, exp_years text
, date_stop text
, time_stop text --time only
, stopduration text
, stop_in_response_to_cfs text
, officer_assignment_key text
, assignment text
, intersection text
, address_block text
, land_mark text
, address_street text
, highway_exit text
, isschool bool
, school_name text
, address_city text
, beat text
, beat_name text
, pid text
, isstudent text
, perceived_limited_english text
, perceived_age text
, perceived_gender text
, gender_nonconforming text
, gend text
, gend_nc text
, perceived_lgbt text
);

--DROP TABLE dmeggs_prod.d_stop_events_sd;
CREATE TABLE dmeggs_prod.d_stop_events_sd

(
stop_id int
, ori text
, agency text
, exp_years int
, event_date date
, event_timestamp_pt timestamp
, stopduration int
, stop_in_response_to_cfs int
, officer_assignment_key int
, assignment text
, intersection text
, address_block text
, land_mark text
, address_street text
, highway_exit text
, isschool bool
, school_name text
, address_city text
, beat text
, beat_name text
, pid int
, isstudent bool
, perceived_limited_english bool
, perceived_age int
, perceived_gender text
, gender_nonconforming bool
, gend int
, gend_nc text
, perceived_lgbt text
, dataset_update_timestamp timestamp
);





INSERT INTO dmeggs_prod.d_stop_events_sd 
SELECT 
  nullif(trim(upper(stop_id)),'')                   as stop_id
, nullif(trim(upper(ori)),'')                       as ori
, nullif(trim(upper(agency)),'')                    as agency
, nullif(trim(upper(exp_years)),'')                 as exp_years
, nullif(trim(upper(date_stop ::date)),'')          as event_date
, concat(date_stop::text,
  concat(' ',
  coalesce(nullif(split_part(trim(time_stop),' ',2),''), '00:00:00')))::timestamp as event_timestamp_pt
, nullif(trim(upper(stopduration)),'')              as stopduration
, nullif(trim(upper(stop_in_response_to_cfs)),'')   as stop_in_response_to_cfs
, nullif(trim(upper(officer_assignment_key)),'')    as officer_assignment_key
, nullif(trim(upper(assignment)),'')                as assignment
, nullif(trim(upper(intersection)),'')              as intersection
, nullif(trim(upper(address_block)),'')             as address_block
, nullif(trim(upper(land_mark)),'')                 as land_mark
, nullif(trim(upper(address_street)),'')            as address_street
, nullif(trim(upper(highway_exit)),'')              as highway_exit
, nullif(trim(upper(isschool)),'')                  as isschool
, nullif(trim(upper(school_name)),'')               as school_name
, nullif(trim(upper(address_city)),'')              as address_city
, nullif(trim(upper(beat)),'')                      as beat
, nullif(trim(upper(beat_name)),'')                 as beat_name
, nullif(trim(upper(pid)),'')                       as pid
, nullif(trim(upper(isstudent)),'')                 as isstudent
, nullif(trim(upper(perceived_limited_english)),'') as perceived_limited_english
, nullif(trim(upper(perceived_age)),'')             as perceived_age
, nullif(trim(upper(perceived_gender)),'')          as perceived_gender
, nullif(trim(upper(gender_nonconforming)),'')      as gender_nonconforming
, nullif(trim(upper(gend)),'')                      as gend
, nullif(trim(upper(gend_nc)),'')                   as gend_nc
, nullif(trim(upper(perceived_lgbt)),'')            as perceived_lgbt
, now()                                             as dataset_update_timestamp
FROM stage.d_stop_events_sd
;





fd_problem_nature_agg_datasd



CREATE TABLE stage.f_sd_category_agg

(
  agency_type text
, address_city text
, problem text
, problem_count int
, month_response int
, year_response int
);



--DROP TABLE dmeggs_prod.f_sd_category_agg;
CREATE TABLE dmeggs_prod.f_sd_category_agg

(
  agency_type text
, address_city text
, problem text
, problem_count int
, month_response int
, year_response int
, dataset_update_timestamp timestamp
);



INSERT INTO dmeggs_prod.f_sd_category_agg 
SELECT 
  agency_type 
, address_city 
, problem 
, problem_count 
, month_response 
, year_response 
, now()                 as dataset_update_timestamp
FROM stage.f_sd_category_agg
;









-----------------------------------------------------------------------------------------------------------------------------




-- Redshift version
CREATE TABLE dwm_prod.d_san_diego_collision_events

(
  report_id varchar(50)
, event_timestamp_pt timestamp
, event_date date
, person_role varchar(150)
, person_injury_lvl varchar(150)
, person_veh_type varchar(150)
, veh_type varchar(150)
, veh_make varchar(150)
, veh_model varchar(150)
, police_beat int
, address_no_primary int 
, address_pd_primary varchar(15)
, address_road_primary varchar(150)
, address_sfx_primary varchar(150)
, address_pd_intersecting varchar(150)
, address_name_intersecting varchar(150)
, address_sfx_intersecting varchar(150)
, violation_section varchar(150)
, violation_type varchar(15)
, charge_desc varchar(150)
, injured int
, killed int
, hit_run_lvl varchar(150)
)

distkey(event_date);