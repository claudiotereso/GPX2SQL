/*******************************************************************************/
/****************   CREATE THE MAIN TABLE **************************************/
/*******************************************************************************/

DROP TABLE IF EXISTS tbGPXTracks
GO
CREATE TABLE tbGPXTracks(
	id int IDENTITY(1,1) NOT NULL,
	gpx xml NOT NULL,
	athlete nvarchar(50) NOT NULL,
	competition nvarchar(50) NOT NULL)
GO


/*******************************************************************************/
/***************   UPLOAD THE GPXs FOR BOTH ATHLETES **************************/
/*******************************************************************************/

INSERT INTO tbGPXTracks
           (gpx
           ,athlete
           ,competition)
SELECT
   x.*,'Luis','Torres Novas Night Trail'
FROM OPENROWSET(
    BULK 'Torres_Novas_Night_Trail_Luis.gpx',
    SINGLE_BLOB) as x
GO
INSERT INTO tbGPXTracks
           (gpx
           ,athlete
           ,competition)
SELECT
   x.*,'Claudio','Torres Novas Night Trail'
FROM OPENROWSET(
    BULK 'Torres_Novas_Night_Trail_Claudio.gpx',
    SINGLE_BLOB) as x
GO

/*******************************************************************************/
/************************   PEAK THE GPX DATA **********************************/
/*******************************************************************************/

WITH XMLNAMESPACES (DEFAULT 'http://www.topografix.com/GPX/1/1')
SELECT athlete,competition,
    nodes.value('@lat','nvarchar(max)') AS lat,nodes.value('@lon','nvarchar(max)') AS lon ,
    nodes.value('*:time[1]','datetime') AS recordedDateTime,
    nodes.value('*:ele[1]','float') AS elevation
FROM tbGPXTracks tracks
CROSS APPLY 
    tracks.gpx.nodes('/gpx/trk/trkseg/trkpt') A(nodes)
GO

/*******************************************************************************/
/**********************   GET GROUPED AND SORTED GPX DATA **********************/
/*******************************************************************************/

WITH XMLNAMESPACES (DEFAULT 'http://www.topografix.com/GPX/1/1')
SELECT ROW_NUMBER() OVER(PARTITION BY athlete,competition ORDER BY recordedDateTime) AS  num, 
    athlete, competition,lat,lon,recordedDateTime,elevation FROM
(
SELECT athlete,competition,
    nodes.value('@lat','nvarchar(max)') AS lat,nodes.value('@lon','nvarchar(max)') AS lon,
    nodes.value('*:time[1]','datetime') AS recordedDateTime,
    nodes.value('*:ele[1]','float') AS elevation
FROM tbGPXTracks tracks 
CROSS APPLY tracks.gpx.nodes('/gpx/trk/trkseg/trkpt') A(nodes)
) results
GO

/*******************************************************************************/
/*************   CREATE a VIEW WITH THE GROUPED AND SORTED GPX DATA ************/
/*******************************************************************************/

DROP VIEW IF EXISTS vwResults
GO
CREATE view vwResults
AS
WITH XMLNAMESPACES (DEFAULT 'http://www.topografix.com/GPX/1/1')
SELECT ROW_NUMBER() OVER(PARTITION BY athlete,competition ORDER BY recordedDateTime) AS  num, 
    athlete, competition,lat,lon,recordedDateTime,elevation FROM
(
SELECT athlete,competition,
    nodes.value('@lat','nvarchar(max)') AS lat,nodes.value('@lon','nvarchar(max)') AS lon,
    nodes.value('time[1]','datetime') AS recordedDateTime,
    nodes.value('ele[1]','float') AS elevation
FROM tbGPXTracks tracks 
CROSS APPLY tracks.gpx.nodes('/gpx/trk/trkseg/trkpt') A(nodes)
) results
GO

/*******************************************************************************/
/*********************   SAVE THE VIEW DATA TO A TABLE *************************/
/*******************************************************************************/

DROP TABLE IF EXISTS tbResults
GO
SELECT num,athlete,competition,lat,lon,recordedDateTime,elevation
INTO tbResults
FROM vwResults
GO


/*******************************************************************************/
/*********************   JOIN TWO CONSEQUTIVE MOMENTS **************************/
/*******************************************************************************/

SELECT before.num fromNum,before.athlete,before.competition,before.recordedDateTime,now.num toNum,now.recordedDateTime
FROM tbResults before
 inner  join tbResults now
  on before.num=now.num-1 and before.competition=now.competition and before.athlete=now.athlete
GO


/*******************************************************************************/
/**************   GET THE DISTANCE JOIN TWO CONSEQUTIVE MOMENTS ****************/
/*******************************************************************************/

SELECT before.num fromNum,before.athlete,before.competition,before.recordedDateTime,now.num toNum,datepart(second,now.recordedDateTime-before.recordedDateTime) elapsedtime,
    GEOGRAPHY::Point(before.lat, before.lon, 4326).STDistance(GEOGRAPHY::Point(now.lat, now.lon, 4326)) distance
FROM tbResults before
inner  join tbResults now
on before.num=now.num-1 and before.competition=now.competition and before.athlete=now.athlete
GO

/*******************************************************************************/
/******   GET THE DISTANCE BETWEEN TWO CONSECUTIVE MOMENTS WITH ALTITUDE *******/
/*******************************************************************************/

SELECT before.num fromNum,before.athlete,before.competition,before.recordedDateTime,now.num toNum,datepart(second,now.recordedDateTime-before.recordedDateTime) elapsedtime,
    sqrt(square(GEOGRAPHY::Point(before.lat, before.lon, 4326).STDistance(GEOGRAPHY::Point(now.lat, now.lon, 4326)))+square(now.elevation-before.elevation))  distance
FROM tbResults before
inner  join tbResults now
    on before.num=now.num-1 and before.competition=now.competition and before.athlete=now.athlete
GO

/*******************************************************************************/
/*******   VIEW FOR DISTANCE BETWEEN TWO CONSECUTIVE MOMENTS WITH ALTITUDE *****/
/*******************************************************************************/

DROP VIEW IF EXISTS vwFinalResults
GO
CREATE view vwFinalResults 
AS

SELECT before.num fromNum,before.athlete,before.competition,before.recordedDateTime,now.num toNum,datepart(second,now.recordedDateTime-before.recordedDateTime) elapsedtime,
    sqrt(square(GEOGRAPHY::Point(before.lat, before.lon, 4326).STDistance(GEOGRAPHY::Point(now.lat, now.lon, 4326)))+square(now.elevation-before.elevation))  distance
FROM tbResults before
inner  join tbResults now
    on before.num=now.num-1 and before.competition=now.competition and before.athlete=now.athlete
GO


/*******************************************************************************/
/****************************   CUMULATIVE DISTANCE AND TIME *******************/
/*******************************************************************************/

SELECT fromNum , competition,athlete,
    SUM(distance) OVER (PARTITION BY competition,athlete ORDER BY fromNum) distance,
    SUM (elapsedtime) OVER (PARTITION BY competition,athlete ORDER BY fromNum) elapsedtime
from vwFinalResults
GO

/*******************************************************************************/
/*************** CREATE VIEW FOR CUMULATIVE DISTANCE AND TIME *******************/
/*******************************************************************************/


DROP VIEW IF EXISTS vwAnalysis
GO
CREATE VIEW vwAnalysis as 
SELECT fromNum , competition,athlete,
    SUM(distance) OVER (PARTITION BY competition,athlete ORDER BY fromNum) distance,
    SUM (elapsedtime) OVER (PARTITION BY competition,athlete ORDER BY fromNum) elapsedtime
from vwFinalResults
GO


/*******************************************************************************/
/*************** GET MAX DISTANCE BETWEEN MY FRIEND AND ME *********************/
/*******************************************************************************/


SELECT TOP 1 claudio.distance,CONVERT(VARCHAR(8), DATEADD(SECOND, claudio.elapsedtime, 0), 108) AS trackTime,
    luis.distance-claudio.distance metersAhead
FROM vwanalysis claudio
INNER JOIN vwanalysis luis
    ON claudio.athlete='Claudio' AND luis.athlete='Luis' 
        AND claudio.competition='Torres Novas Night Trail' AND luis.competition='Torres Novas Night Trail' 
        AND claudio.elapsedTime=luis.elapsedTime
ORDER BY luis.distance-claudio.distance DESC


