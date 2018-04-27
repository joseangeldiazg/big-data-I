CREATE TABLE IF NOT EXISTS Complaints(DateReceived STRING, ProductName STRING, SubProduct STRING,Issue STRING, SubIssue STRING,ConsumerComplaintNarrative STRING,CompanyPublicResponse String, Company STRING, StateName STRING,ZipCode INT, Tags STRING, ConsumerConsentProvided STRING,SubmittedVia STRING, DateSenttoCompany STRING, CompanyResponsetoConsumer STRING, TimelyResponse STRING,ConsumerDisputed STRING, ComplaintID INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,' STORED AS TEXTFILE;

LOAD DATA INPATH '/user/impala/CD_DNI/input/ConsumerComplaints.csv' OVERWRITE INTO TABLE complaints;

SELECT DIFFERENT SubmittedVia FROM complaints;

SELECT COUNT(*), SubmittedVia FROM complaints GROUP BY SubmittedVia HAVING COUNT(*) > 300;

SELECT COUNT(*), SubmittedVia FROM complaints GROUP BY SubmittedVia WHERE SubmittedVia IN ('Web', 'Phone', 'Fax', 'Postal mail') HAVING COUNT(*) > 300 ORDER BY 1 DESC;Close

SELECT COUNT(*), SatateName FROM complaints GROUP BY SubmittedVia WHERE SubmittedVia = 'Web' HAVING COUNT(*) < 10;

SELECT COUNT(*), SatateName FROM complaints GROUP BY SubmittedVia WHERE SubmittedVia = 'Web' HAVING COUNT(*) BETWEEN 100 and 200;
