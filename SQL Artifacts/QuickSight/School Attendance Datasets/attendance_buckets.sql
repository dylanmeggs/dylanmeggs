/*
We won't have student-course enrollment data for this example.
For this reason I am left joining demographics dataset to attendance dataset instead of the other way around.
We are excluding students with perfect attendance across all courses.
We will be evaluating only students with 1 or more absences in 1 or more courses.
Our Absence Rate calculation will be deflated at higher levels of granularity, because it only contains student-course pairs where the student had at least one absence. 
*/

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
    -- Some absences likely to be excluded from aggregation
    -- Each district might have its own rules on this?
    -- Ideally we handle this upstream instead of hard coding
    WHERE att.absence_reason not in (
        'Field Trip', --(school-sanctioned)
        'Home Tutoring', --(alternate instruction)
        'In-School Suspension', --(still under supervision)
        'Present but Remote', --(attended remotely)
        'Early Dismissal', --(School let everyone out early)
        --'Doctor Appointment', 'Dentist Appointment', --(excused, brief)
        'Late - Excused', --(still attended)
    )
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
      )
      
  SELECT
  	  'Overall' 					          as granularity,
      cl.student_guid,
      cl.school_year,
      cast(substr(nullif(trim(cl.school_year),''),1,4) as integer) as start_year,
      'All' 						          as course,
      null 						              as course_id,
      --No longer using absence reason, but I'm leaving the field here because the removal of fields can break dashboards and force a rebuild
      null                                    as absence_reason,
      cl.race,
      cl.primary_language,
      cl.economically_disadvantaged,
      cl."504",
      cl.home_school_district,
      cl.is_specialed,
      cl.building,
      cl.school_level,
      cl.grade_level,
      sum(cl.course_absences) 		        as absence_count,
      sum(cl.course_opportunities) 	        as opportunities,
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
      cl.grade_level
  
  UNION ALL
  
    SELECT
  	  'Course-Level' 				        as granularity,
      cl.student_guid,
      cl.school_year,
      cast(substr(nullif(trim(cl.school_year),''),1,4) as integer) as start_year,
      cl.course_name 				        as course,
      cl.course_id,
      null                                  as absence_reason,
      cl.race,
      cl.primary_language,
      cl.economically_disadvantaged,
      cl."504",
      cl.home_school_district,
      cl.is_specialed,
      cl.building,
      cl.school_level,
      cl.grade_level,
      cl.course_absences 			        as absence_count,
      cl.course_opportunities 	            as opportunities,
      ROUND((cl.course_absences * 1.000000 / cl.course_opportunities),6) as absence_rate,
      case
      	when ROUND((cl.course_absences * 1.000000 / cl.course_opportunities),6) < .05
        	then 'Low Risk'
        when ROUND((cl.course_absences * 1.000000 / cl.course_opportunities),6) < .10
        	then 'At Risk'
        else 'Chronically Absent'
      end 							            as risk_bucket
      
  FROM course_level_agg cl