X
,Y
,OBJECTID
,ICN
,CrashID
,CountyCode
,CrashYr
,CrashMonth
,CrashDay
,NumberOfVehicles
,DayOfWeekCode
,CrashHour
,CityCode
,CityClassCode
,Township
,CollisionTypeCode
,TotalFatals
,TotalInjured
,NoInjuries
,AInjuries
,BInjuries
,CInjuries
,CrashSeverity
,AgencyCode
,RouteNumber
,Milestation
,ClassOfTrafficwayCode
,NHS
,TrafficControlDeviceCode
,RoadSurfaceConditionCode
,RoadDefectsCode
,LightConditionCode
,WeatherCode
,Cause1Code
,Cause2Code
,RailroadCrossingNumber
,TimeOfCrash
,TrafficControlDeviceCondCode
,IntersectionRelated
,HitAndRun
,CrashDate
,NumberOfLanes
,AlignmentCode
,TrafficwayDescriptionCode
,RoadwayFunctionalClassCode
,WorkZoneRelated
,City_Township_Flag
,TSCrashCoordinateX
,TSCrashCoordinateY
,TSCrashLatitude
,TSCrashLongitude
,CrashReportCounty
,DayOfWeek
,TypeOfFirstCrash
,CityName
,CityClass
,ClassOfTrafficway
,Cause1
,Cause2
,TrafficControlDevice
,TrafficControlDeviceCond
,RoadSurfaceCond
,RoadDefects
,CrashInjurySeverity
,LightingCond
,WeatherCond
,RoadAlignment
,TrafficwayDescrip
,RoadwayFunctionalClass
,InvestigatingAgencyDescrip
,CrashSeverityCd
,DamagedProperty1
,DamagedProperty2
,AgencyReportYear
,AgencyReportNumber
,SFE
,DidCrashOccurInWorkZone
,WorkZoneTypeWereWorkersPresent,WorkZone
,AccessControl
,FlowCondition
,DidInvolveSecondaryCrash
,Toll
,UrbanRural


--drop table if exists stage.d_idot_crash_2022
create table stage.d_idot_crash_2022 
(
X  text
,Y text
,OBJECTID text
,ICN text
,CrashID text
,CountyCode text
,CrashYr text
,CrashMonth text
,CrashDay text
,NumberOfVehicles text
,DayOfWeekCode text
,CrashHour text
,CityCode text
,CityClassCode text
,Township text
,CollisionTypeCode text
,TotalFatals text
,TotalInjured text
,NoInjuries text
,AInjuries text
,BInjuries text
,CInjuries text
,CrashSeverity text
,AgencyCode text
,RouteNumber text
,Milestation text
,ClassOfTrafficwayCode text
,NHS text
,TrafficControlDeviceCode text
,RoadSurfaceConditionCode text
,RoadDefectsCode text
,LightConditionCode text
,WeatherCode text
,Cause1Code text
,Cause2Code text
,RailroadCrossingNumber text
,TimeOfCrash text
,TrafficControlDeviceCondCode text
,IntersectionRelated text
,HitAndRun text
,CrashDate text
,NumberOfLanes text
,AlignmentCode text
,TrafficwayDescriptionCode text
,RoadwayFunctionalClassCode text
,WorkZoneRelated text
,City_Township_Flag text
,TSCrashCoordinateX text
,TSCrashCoordinateY text
,TSCrashLatitude text
,TSCrashLongitude text
,CrashReportCounty text
,DayOfWeek text
,TypeOfFirstCrash text
,CityName text
,CityClass text
,ClassOfTrafficway text
,Cause1 text
,Cause2 text
,TrafficControlDevice text
,TrafficControlDeviceCond text
,RoadSurfaceCond text
,RoadDefects text
,CrashInjurySeverity text
,LightingCond text
,WeatherCond text
,RoadAlignment text
,TrafficwayDescrip text
,RoadwayFunctionalClass text
,InvestigatingAgencyDescrip text
,CrashSeverityCd text
,DamagedProperty1 text
,DamagedProperty2 text
,AgencyReportYear text
,AgencyReportNumber text
,SFE text
,DidCrashOccurInWorkZone text
,WorkZoneTypeWere text
,WorkersPresent text
,WorkZone text
,AccessControl text
,FlowCondition text
,DidInvolveSecondaryCrash text
,Toll text
,UrbanRural text
);


--drop table if exists dwmeggs_prod.d_idot_crash_2022
create table dwmeggs_prod.d_idot_crash_2022 
(
lat point
,lng point
,OBJECTID text
,ICN  int
,CrashID int
,CountyCode int
,CrashYr int
,CrashMonth int
,CrashDay int
,NumberOfVehicles int
,DayOfWeekCode int
,CrashHour int
,CityCode int
,CityClassCode int
,Township int
,CollisionTypeCode int
,TotalFatals int
,TotalInjured int
,NoInjuries int
,AInjuries int
,BInjuries int
,CInjuries int
,CrashSeverity
,AgencyCode int
,RouteNumber int
,Milestation text
,ClassOfTrafficwayCode text
,NHS text
,TrafficControlDeviceCode int
,RoadSurfaceConditionCode int
,RoadDefectsCode int
,LightConditionCode int
,WeatherCode int
,Cause1Code int
,Cause2Code int
,RailroadCrossingNumber text
,TimeOfCrash text
,TrafficControlDeviceCondCode text
,IntersectionRelated text
,HitAndRun text
,CrashDate timestamp
,NumberOfLanes text
,AlignmentCode text
,TrafficwayDescriptionCode int
,RoadwayFunctionalClassCode int
,WorkZoneRelated text
,City_Township_Flag text
,TSCrashCoordinateX text
,TSCrashCoordinateY text
,TSCrashLatitude point
,TSCrashLongitude point
,CrashReportCounty text
,DayOfWeek text
,TypeOfFirstCrash text
,CityName text
,CityClass text
,ClassOfTrafficway text
,Cause1 text
,Cause2 text
,TrafficControlDevice text
,TrafficControlDeviceCond text
,RoadSurfaceCond text
,RoadDefects text
,CrashInjurySeverity text
,LightingCond text
,WeatherCond text
,RoadAlignment text
,TrafficwayDescrip text
,RoadwayFunctionalClass text
,InvestigatingAgencyDescrip text
,CrashSeverityCd int
,DamagedProperty1 text
,DamagedProperty2 text
,AgencyReportYear int
,AgencyReportNumber text
,SFE text
,DidCrashOccurInWorkZone text
,WorkZoneType text
,WereWorkersPresent text
,WorkZone text
,AccessControl text
,FlowCondition text
,DidInvolveSecondaryCrash text
,Toll text
,UrbanRural text
);


INSERT INTO dwmeggs_prod.d_idot_crash_2022 values
SELECT 
nullif(trim(upper(X)),'')                           as lat
,nullif(trim(upper(Y)),'')                          as lng
,nullif(trim(upper(OBJECTID)),'')                   as OBJECTID
,nullif(trim(upper(ICN)),'')                        as ICN
,nullif(trim(upper(CrashID)),'')                    as CrashID
,nullif(trim(upper(CountyCode)),'')                 as CountyCode
,nullif(trim(upper(CrashYr)),'')                    as CrashYr
,nullif(trim(upper(CrashMonth)),'')                 as CrashMonth
,nullif(trim(upper(CrashDay)),'')                   as CrashDay
,nullif(trim(upper(NumberOfVehicles)),'')           as NumberOfVehicles
,nullif(trim(upper(DayOfWeekCode)),'')              as DayOfWeekCode
,nullif(trim(upper(CrashHour)),'')                  as CrashHour
,nullif(trim(upper(CityCode)),'')                   as CityCode
,nullif(trim(upper(CityClassCode)),'')              as CityClassCode
,nullif(trim(upper(Township)),'')                   as Township
,nullif(trim(upper(CollisionTypeCode)),'')          as CollisionTypeCode
,nullif(trim(upper(TotalFatals)),'')                as TotalFatals
,nullif(trim(upper(TotalInjured)),'')               as TotalInjured
,nullif(trim(upper(NoInjuries)),'')                 as NoInjuries
,nullif(trim(upper(AInjuries)),'')                  as AInjuries
,nullif(trim(upper(BInjuries)),'')                  as BInjuries
,nullif(trim(upper(CInjuries)),'')                  as CInjuries
,nullif(trim(upper(CrashSeverity)),'')              as CrashSeverity
,nullif(trim(upper(AgencyCode)),'')                 as AgencyCode
,nullif(trim(upper(RouteNumber)),'')                as RouteNumber
,nullif(trim(upper(Milestation)),'')                as Milestation
,nullif(trim(upper(ClassOfTrafficwayCode)),'')      as ClassOfTrafficwayCode
,nullif(trim(upper(NHS)),'')                        as NHS
,nullif(trim(upper(TrafficControlDeviceCode)),'')   as TrafficControlDeviceCode
,nullif(trim(upper(RoadSurfaceConditionCode)),'')   as RoadSurfaceConditionCode
,nullif(trim(upper(RoadDefectsCode)),'')            as RoadDefectsCode
,nullif(trim(upper(LightConditionCode)),'')         as LightConditionCode
,nullif(trim(upper(WeatherCode)),'')                as WeatherCode
,nullif(trim(upper(Cause1Code)),'')                 as Cause1Code
,nullif(trim(upper(Cause2Code)),'')                 as Cause2Code
,nullif(trim(upper(RailroadCrossingNumber)),'')     as RailroadCrossingNumber
,nullif(trim(upper(TimeOfCrash)),'')                as TimeOfCrash
,nullif(trim(upper(TrafficControlDeviceCondCode)),'') as TrafficControlDeviceCondCode
,nullif(trim(upper(IntersectionRelated)),'')        as IntersectionRelated
,nullif(trim(upper(HitAndRun)),'')                  as HitAndRun
,nullif(trim(upper(CrashDate)),'')                  as CrashDate
,nullif(trim(upper(NumberOfLanes)),'')              as NumberOfLanes
,nullif(trim(upper(AlignmentCode)),'')              as AlignmentCode
,nullif(trim(upper(TrafficwayDescriptionCode)),'')  as TrafficwayDescriptionCode
,nullif(trim(upper(RoadwayFunctionalClassCode)),'') as RoadwayFunctionalClassCode
,nullif(trim(upper(WorkZoneRelated)),'')            as WorkZoneRelated
,nullif(trim(upper(City_Township_Flag)),'')         as City_Township_Flag
,nullif(trim(upper(TSCrashCoordinateX)),'')         as TSCrashCoordinateX
,nullif(trim(upper(TSCrashCoordinateY)),'')         as TSCrashCoordinateY
,nullif(trim(upper(TSCrashLatitude)),'')            as TSCrashLatitude
,nullif(trim(upper(TSCrashLongitude)),'')           as TSCrashLongitude
,nullif(trim(upper(CrashReportCounty)),'')          as CrashReportCounty
,nullif(trim(upper(DayOfWeek)),'')                  as DayOfWeek
,nullif(trim(upper(TypeOfFirstCrash)),'')           as TypeOfFirstCrash
,nullif(trim(upper(CityName)),'')                   as CityName
,nullif(trim(upper(CityClass)),'')                  as CityClass
,nullif(trim(upper(ClassOfTrafficway)),'')          as ClassOfTrafficway
,nullif(trim(upper(Cause1)),'')                     as Cause1
,nullif(trim(upper(Cause2)),'')                     as Cause2
,nullif(trim(upper(TrafficControlDevice)),'')       as TrafficControlDevice
,nullif(trim(upper(TrafficControlDeviceCond)),'')   as TrafficControlDeviceCond
,nullif(trim(upper(RoadSurfaceCond)),'')            as RoadSurfaceCond
,nullif(trim(upper(RoadDefects)),'')                as RoadDefects
,nullif(trim(upper(CrashInjurySeverity)),'')        as CrashInjurySeverity
,nullif(trim(upper(LightingCond)),'')               as LightingCond
,nullif(trim(upper(WeatherCond)),'')                as WeatherCond
,nullif(trim(upper(RoadAlignment)),'')              as RoadAlignment
,nullif(trim(upper(TrafficwayDescrip)),'')          as TrafficwayDescrip
,nullif(trim(upper(RoadwayFunctionalClass)),'')     as RoadwayFunctionalClass
,nullif(trim(upper(InvestigatingAgencyDescrip)),'') as InvestigatingAgencyDescrip
,nullif(trim(upper(CrashSeverityCd)),'')            as CrashSeverityCd
,nullif(trim(upper(DamagedProperty1)),'')           as DamagedProperty1
,nullif(trim(upper(DamagedProperty2)),'')           as DamagedProperty2
,nullif(trim(upper(AgencyReportYear)),'')           as AgencyReportYear
,nullif(trim(upper(AgencyReportNumber)),'')         as AgencyReportNumber
,nullif(trim(upper(SFE)),'')                        as SFE
,nullif(trim(upper(DidCrashOccurInWorkZone)),'')    as DidCrashOccurInWorkZone
,nullif(trim(upper(WorkZoneType)),'')               as WorkZoneType
,nullif(trim(upper(WereWorkersPresent)),'')         as WereWorkersPresent
,nullif(trim(upper(WorkZone)),'')                   as WorkZone
,nullif(trim(upper(AccessControl)),'')              as AccessControl
,nullif(trim(upper(FlowCondition)),'')              as FlowCondition
,nullif(trim(upper(DidInvolveSecondaryCrash)),'')   as DidInvolveSecondaryCrash
,nullif(trim(upper(Toll)),'')                       as Toll
,nullif(trim(upper(UrbanRural)),'')                 as UrbanRural
FROM stage.d_idot_crash_2022