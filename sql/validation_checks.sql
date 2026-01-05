Use [25f_cst2112];

-- Show total count of raw table
Select COUNT(*) from dbo.raw_nar;

-- Top 5 rows from raw table
Select top 5 * from dbo.raw_nar; 
-- Count of normalized tables
SELECT 
    (SELECT COUNT(*) FROM Province) AS ProvinceCount,
    (SELECT COUNT(*) FROM BuildingUsage) AS BuildingUsageCount,
    (SELECT COUNT(*) FROM CensusSubdivision) AS CensusSubdivisionCount,
    (SELECT COUNT(*) FROM PostalCode) AS PostalCodeCount,
    (SELECT COUNT(*) FROM Location) AS LocationCount,
    (SELECT COUNT(*) FROM Address) AS AddressCount;

