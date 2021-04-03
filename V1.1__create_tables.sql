DROP TABLE IF EXISTS TestResult;
DROP TABLE IF EXISTS Participant;
DROP TABLE IF EXISTS EduInstitution;
DROP TABLE IF EXISTS Location;


CREATE TABLE Location (
    loc_id SERIAL PRIMARY KEY,
    TerTypeName VARCHAR(40),
    TerName VARCHAR(255),
    AreaName VARCHAR(255),
    RegName VARCHAR(255)
);


CREATE TABLE EduInstitution (
    EOName VARCHAR(255) PRIMARY KEY,
    EOTypeName VARCHAR(255),
    loc_id SERIAL REFERENCES Location(loc_id),
    EOParent VARCHAR(255)
);


CREATE TABLE Participant (
    OutID VARCHAR(40) PRIMARY KEY,
    birth INT,
	SexTypeName VARCHAR(40),
    loc_id SERIAL REFERENCES Location(loc_id),
    ParticipType VARCHAR(255),
    ClassProfileName VARCHAR(255),
    ClassLangName VARCHAR(255),
    EOName VARCHAR(255) REFERENCES EduInstitution(EOName)
);


CREATE TABLE TestResult (
    OutID VARCHAR(40) REFERENCES Participant(OutID),
    TestName VARCHAR(255),
    Year INT,
    Lang VARCHAR(255),
    TestStatus VARCHAR(255),
    DPALevel VARCHAR(255),
    Ball100 REAL,
    Ball12 REAL,
    Ball REAL,
    AdaptScale VARCHAR(10),
    PTName VARCHAR(255) REFERENCES EduInstitution(EOName),
    PRIMARY KEY(OutID, TestName, Year)
);
