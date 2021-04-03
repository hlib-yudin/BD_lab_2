-- сутність "Локація" (місцезнаходження)
INSERT INTO Location (TerName, AreaName, RegName)
-- територія реєстрації/проживання учасника
SELECT DISTINCT       TerName,       AreaName,       RegName       FROM tbl_zno_data
-- територія навчального закладу
UNION SELECT DISTINCT EOTerName,     EOAreaName,     EORegName     FROM tbl_zno_data
-- території пунктів проведення ЗНО
UNION SELECT DISTINCT ukrPTTerName,  ukrPTAreaName,  ukrPTRegName  FROM tbl_zno_data
UNION SELECT DISTINCT mathPTTerName, mathPTAreaName, mathPTRegName FROM tbl_zno_data
UNION SELECT DISTINCT histPTTerName, histPTAreaName, histPTRegName FROM tbl_zno_data
UNION SELECT DISTINCT physPTTerName, physPTAreaName, physPTRegName FROM tbl_zno_data
UNION SELECT DISTINCT chemPTTerName, chemPTAreaName, chemPTRegName FROM tbl_zno_data
UNION SELECT DISTINCT bioPTTerName,  bioPTAreaName,  bioPTRegName  FROM tbl_zno_data
UNION SELECT DISTINCT geoPTTerName,  geoPTAreaName,  geoPTRegName  FROM tbl_zno_data
UNION SELECT DISTINCT engPTTerName,  engPTAreaName,  engPTRegName  FROM tbl_zno_data
UNION SELECT DISTINCT fraPTTerName,  fraPTAreaName,  fraPTRegName  FROM tbl_zno_data
UNION SELECT DISTINCT deuPTTerName,  deuPTAreaName,  deuPTRegName  FROM tbl_zno_data
UNION SELECT DISTINCT spaPTTerName,  spaPTAreaName,  spaPTRegName  FROM tbl_zno_data;


-- видалити рядки з NULL
DELETE FROM Location WHERE TerName IS NULL;
/*
-- можливі наступні пари рядків: в одному рядку AreaName = м. Київ,
-- а в іншому -- AreaName = м. Київ. Шевченківський район міста
-- для таких пар рядків треба видалити той рядок, атрибут AreaName якого містить також і район
-- визначити такі рядки просто: атрибут AreaName міститиме дві крапки
-- (приклад: м. Київ. Шевченківський район міста -- містить дві крапки)
DELETE FROM Location loc1
-- перша умова -- AreaName містить дві крапки
WHERE (CHAR_LENGTH(loc1.AreaName) - CHAR_LENGTH(REPLACE(loc1.AreaName, '.', ''))) = 2 AND
-- друга умова -- вже існують рядки з заданою локацією (наприклад, у них AreaName = м. Київ,
-- а TerName = Шевченківський рядок міста)
    (SELECT COUNT(*) FROM Location loc2 
    WHERE loc2.AreaName = split_part(loc1.AreaName, '.', 1) || '.' || split_part(loc1.AreaName, '.', 2)
        AND loc2.TerName = ltrim(split_part(loc1.AreaName, '.', 3))) >= 1 ; 
*/

-- вставляємо інформацію про тип населеного пункту (місто чи село)
UPDATE Location 
SET terTypeName = tbl_zno_data.terTypeName
FROM tbl_zno_data 
WHERE tbl_zno_data.TerName  = Location.TerName AND
    tbl_zno_data.AreaName   = Location.AreaName AND
    tbl_zno_data.RegName    = Location.RegName;


-- сутність "Навчальний заклад"
INSERT INTO EduInstitution(EOName, EOTypeName, loc_id, EOParent)
SELECT DISTINCT ON (allEduInfo.eduName)
	allEduInfo.eduName, 
	tbl_zno_data.EOTypeName, 
	Location.loc_id,
	tbl_zno_data.EOParent
FROM (
    -- інформація про всі навчальні заклади: назва та місцезнаходження
    select distinct * 
    FROM (
        SELECT DISTINCT EOName, EOTerName, EOAreaName, EORegName FROM tbl_zno_data
        UNION SELECT DISTINCT ukrPTName,  ukrPTTerName,  ukrPTAreaName,  ukrPTRegName  FROM tbl_zno_data
        UNION SELECT DISTINCT mathPTName, mathPTTerName, mathPTAreaName, mathPTRegName FROM tbl_zno_data
        UNION SELECT DISTINCT histPTName, histPTTerName, histPTAreaName, histPTRegName FROM tbl_zno_data
        UNION SELECT DISTINCT physPTName, physPTTerName, physPTAreaName, physPTRegName FROM tbl_zno_data
        UNION SELECT DISTINCT chemPTName, chemPTTerName, chemPTAreaName, chemPTRegName FROM tbl_zno_data
        UNION SELECT DISTINCT bioPTName,  bioPTTerName,  bioPTAreaName,  bioPTRegName  FROM tbl_zno_data
        UNION SELECT DISTINCT geoPTName,  geoPTTerName,  geoPTAreaName,  geoPTRegName  FROM tbl_zno_data
        UNION SELECT DISTINCT engPTName,  engPTTerName,  engPTAreaName,  engPTRegName  FROM tbl_zno_data
        UNION SELECT DISTINCT fraPTName,  fraPTTerName,  fraPTAreaName,  fraPTRegName  FROM tbl_zno_data
        UNION SELECT DISTINCT deuPTName,  deuPTTerName,  deuPTAreaName,  deuPTRegName  FROM tbl_zno_data
        UNION SELECT DISTINCT spaPTName,  spaPTTerName,  spaPTAreaName,  spaPTRegName  FROM tbl_zno_data
    ) as temp
) AS allEduInfo (eduName, TerName, AreaName, RegName)
-- join tbl_zno_data, щоб додати інформацію про тип навчального закладу та про EOParent
LEFT JOIN tbl_zno_data ON 
	allEduInfo.EduName = tbl_zno_data.EOName
-- join Location, щоб додати інформацію про loc_id
LEFT JOIN Location ON
	allEduInfo.TerName = Location.TerName AND
	allEduInfo.AreaName = Location.AreaName AND
	allEduInfo.RegName = Location.RegName
WHERE allEduInfo.eduName IS NOT NULL;


-- сутність "Учасник"
INSERT INTO Participant (OutID, birth, SexTypeName, loc_id, 
    ParticipType, ClassProfileName, ClassLangName, EOName)
SELECT DISTINCT ON (OutID) OutID, birth, SexTypeName, loc_id, 
    RegTypeName, ClassProfileName, ClassLangName, EOName
FROM tbl_zno_data INNER JOIN Location
ON tbl_zno_data.TerTypeName = Location.TerTypeName
    AND tbl_zno_data.TerName = Location.TerName
    AND tbl_zno_data.AreaName = Location.AreaName
    AND tbl_zno_data.RegName = Location.RegName;


-- сутність "Результат тесту"
INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, ukrTest, Year, NULL, ukrTestStatus, NULL, 
    ukrBall100, ukrBall12, ukrBall, ukrAdaptScale, UkrPTName
FROM tbl_zno_data
WHERE ukrTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, histTest, Year, histLang, histTestStatus, NULL, 
    histBall100, histBall12, histBall, NULL, histPTName
FROM tbl_zno_data
WHERE histTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, mathTest, Year, mathLang, mathTestStatus, NULL, 
    mathBall100, mathBall12, mathBall, NULL, mathPTName
FROM tbl_zno_data
WHERE mathTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, physTest, Year, physLang, physTestStatus, NULL, 
    physBall100, physBall12, physBall, NULL, physPTName
FROM tbl_zno_data
WHERE physTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, chemTest, Year, chemLang, chemTestStatus, NULL, 
    chemBall100, chemBall12, chemBall, NULL, chemPTName
FROM tbl_zno_data
WHERE chemTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, bioTest, Year, bioLang, bioTestStatus, NULL, 
    bioBall100, bioBall12, bioBall, NULL, bioPTName
FROM tbl_zno_data
WHERE bioTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, geoTest, Year, geoLang, geoTestStatus, NULL, 
    geoBall100, geoBall12, geoBall, NULL, geoPTName
FROM tbl_zno_data
WHERE geoTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, engTest, Year, NULL, engTestStatus, engDPALevel, 
    engBall100, engBall12, engBall, NULL, engPTName
FROM tbl_zno_data
WHERE engTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, fraTest, Year, NULL, fraTestStatus, fraDPALevel, 
    fraBall100, fraBall12, fraBall, NULL, fraPTName
FROM tbl_zno_data
WHERE fraTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, deuTest, Year, NULL, deuTestStatus, deuDPALevel, 
    deuBall100, deuBall12, deuBall, NULL, deuPTName
FROM tbl_zno_data
WHERE deuTest IS NOT NULL;


INSERT INTO TestResult (OutID, TestName, Year, Lang, TestStatus,
    DPALevel, Ball100, Ball12, Ball, AdaptScale, PTName)
SELECT OutID, spaTest, Year, NULL, spaTestStatus, spaDPALevel, 
    spaBall100, spaBall12, spaBall, NULL, spaPTName
FROM tbl_zno_data
WHERE spaTest IS NOT NULL;


