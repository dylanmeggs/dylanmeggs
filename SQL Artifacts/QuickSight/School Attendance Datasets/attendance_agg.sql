WITH course_level_agg as (
SELECT
      att.student_guid,
      att.school_year,
      att.course_name,
      att.course_id,
      --att.absence_reason,
      dem.race,
      dem.primary_language,
      dem.economically_disadvantaged,
      dem."504",
      dem.home_school_district,
      dem.is_specialed,
      dem.building,
      dem.school_level,
      dem.grade_level,
      count(att.student_guid) as course_absences,
      max(max_possible_days_attended_for_course) as course_opportunities
    FROM student_course_attendance att
    LEFT JOIN student_demographics dem
    ON dem.student_guid = att.student_guid
    GROUP BY att.student_guid,
      att.school_year,
      att.course_name,
      att.course_id,
      --att.absence_reason,
      dem.race,
      dem.primary_language,
      dem.economically_disadvantaged,
      dem."504",
      dem.home_school_district,
      dem.is_specialed,
      dem.building,
      dem.school_level,
      dem.grade_level
      ),
student_year_agg as (
  SELECT
  	  'Overall' 					          as granularity,
      cl.student_guid,
      cl.school_year,
      cast(substr(nullif(trim(cl.school_year),''),1,4) as integer) as start_year,
      'All' 						          as course,
      null 						              as course_id,
      cl.race,
      cl.primary_language,
      cl.economically_disadvantaged,
      cl."504",
      cl.home_school_district,
      cl.is_specialed,
      cl.building,
      cl.school_level,
      cl.grade_level,
      sum(cl.course_absences) 		  as absence_count,
      sum(cl.course_opportunities) 	as opportunities,
      ROUND((sum(cl.course_absences) * 1.000000 / sum(cl.course_opportunities)),6) as absence_rate,
      case
      	when ROUND((sum(cl.course_absences) * 1.000000 / sum(cl.course_opportunities)),6) < .05
        	then 'Low Risk'
        when ROUND((sum(cl.course_absences) * 1.000000 / sum(cl.course_opportunities)),6) < .10
        	then 'At Risk'
        else 'Chronically Absent'
      end 							            as risk_bucket
  FROM course_level_agg cl
  GROUP BY cl.student_guid,
      cl.school_year,
      cl.race,
      cl.primary_language,
      cl.economically_disadvantaged,
      cl."504",
      cl.home_school_district,
      cl.is_specialed,
      cl.building,
      cl.school_level,
      cl.grade_level),
bucket_agg as (
    SELECT
    school_year,
    count(distinct case when risk_bucket = 'Low Risk' then student_guid end) as total_low_risk,
    count(distinct case when risk_bucket = 'At Risk' then student_guid end) as total_at_risk,
    count(distinct case when risk_bucket = 'Chronically Absent' then student_guid end) as total_chronically_absent,
    count(distinct student_guid) as total_students
    FROM student_year_agg
    GROUP BY 1
),
union_agg as (
SELECT
    sy.school_year,
    'Economic Status' as segment_type,
    sy.economically_disadvantaged as segment,
    sum(case when sy.risk_bucket = 'Low Risk' then 1 else 0 end) as low_risk,
    sum(case when sy.risk_bucket = 'At Risk' then 1 else 0 end) as at_risk,
    sum(case when sy.risk_bucket = 'Chronically Absent' then 1 else 0 end) as chronically_absent,
    count(*) as total_in_segment


FROM student_year_agg sy
GROUP BY sy.school_year, sy.economically_disadvantaged

UNION ALL

SELECT
    sy.school_year,
    'Ethnicity/Race' as segment_type,
    sy.race as segment,
    sum(case when sy.risk_bucket = 'Low Risk' then 1 else 0 end) as low_risk,
    sum(case when sy.risk_bucket = 'At Risk' then 1 else 0 end) as at_risk,
    sum(case when sy.risk_bucket = 'Chronically Absent' then 1 else 0 end) as chronically_absent,
    count(*) as total_in_segment
    

FROM student_year_agg sy
GROUP BY sy.school_year, sy.race

UNION ALL

SELECT
    sy.school_year,
    '504' as segment_type,
    sy."504" as segment,
    sum(case when sy.risk_bucket = 'Low Risk' then 1 else 0 end) as low_risk,
    sum(case when sy.risk_bucket = 'At Risk' then 1 else 0 end) as at_risk,
    sum(case when sy.risk_bucket = 'Chronically Absent' then 1 else 0 end) as chronically_absent,
    count(*) as total_in_segment

FROM student_year_agg sy
GROUP BY sy.school_year, sy."504"
)

select 
    u.school_year,
    u.segment_type,
    u.segment,
    u.low_risk,
    u.at_risk,
    u.chronically_absent,
    u.total_in_segment,
    b.total_low_risk,
    b.total_at_risk,
    b.total_chronically_absent, 
    b.total_students,
    round(u.low_risk * 1.00 / total_low_risk,2) as perc_of_low_risk,
    round(u.at_risk * 1.00 / total_at_risk,2) as perc_of_at_risk,
    round(u.chronically_absent * 1.00 / total_chronically_absent,2) as perc_of_chronically_absent,
    round(u.total_in_segment * 1.00 / b.total_students,2) as percent_of_total
from union_agg u
join bucket_agg b
on u.school_year = b.school_year
group by 1,2,3,4,5,6,7,8,9,10,11