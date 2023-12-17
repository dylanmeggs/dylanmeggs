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
  report_id 
, date_time             as event_timestamp_pt
, date_time::date       as event_date
, person_role 
, person_injury_lvl 
, person_veh_type 
, veh_type 
, veh_make 
, veh_model 
, police_beat 
, address_no_primary  
, address_pd_primary 
, address_road_primary 
, address_sfx_primary 
, address_pd_intersecting 
, address_name_intersecting 
, address_sfx_intersecting 
, violation_section 
, violation_type 
, charge_desc 
, injured
, killed
, hit_run_lvl
, now()                 as dataset_update_timestamp
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
stop_id
, ori
, agency
, exp_years
, date_stop ::date        as event_date
, concat(date_stop::text,concat(' ', coalesce(nullif(split_part(trim(time_stop),' ',2),''), '00:00:00')))::timestamp as event_timestamp_pt
, stopduration
, stop_in_response_to_cfs
, officer_assignment_key
, assignment
, intersection
, address_block
, land_mark
, address_street
, highway_exit
, isschool
, school_name
, address_city
, beat
, beat_name
, pid
, isstudent
, perceived_limited_english
, perceived_age
, perceived_gender
, gender_nonconforming
, gend
, gend_nc
, perceived_lgbt
, now()                 as dataset_update_timestamp
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