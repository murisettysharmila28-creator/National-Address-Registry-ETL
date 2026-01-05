"""
GA5 - National Address Register (NAR)

- Step 1: Load all Address_*.csv files into RAW table raw_nar
          All attributes are VARCHAR(300) to avoid conversion errors.
- Step 2: Create relational 3NF tables:
          Province, BuildingUsage, CensusSubdivision, PostalCode, Location, Address
- Step 3: Populate relational tables from raw_nar.

NOTE on MAIL_STEET_DIR vs MAIL_STREET_DIR:
  The CSV header uses MAIL_STEET_DIR (typo).
  The normalized Address table uses MAIL_STREET_DIR (correct).
  In the ETL we map Address.MAIL_STREET_DIR = raw_nar.MAIL_STEET_DIR.
"""

import pyodbc
import csv
import os



# --------------------------------------------------------
# 1. Connection
# --------------------------------------------------------
def get_connection():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=10.100.17.147,34503;"
        "DATABASE=25f_cst2112;"
        "UID=std_10;"
        "PWD=****;"
    )
    return conn

# --------------------------------------------------------
# 2. Bulk-insert helper 
# --------------------------------------------------------
from datetime import datetime
def pushlist(cursor, sql, data, source_file):
    """
    Insert rows in batches of 1000 

    data: list of lists (rows) from csv.reader
    """
    element = 0
    rows = ""
    comma = ""

    for row in data:
        # Add timestamp to every row (first column)
        load_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row_with_meta = [load_time, source_file] + row
        # Replace single quotes with ` to avoid breaking SQL strings
        values = "('" + "','".join(str(val).replace("'", "`") for val in row_with_meta) + "')"
        rows = rows + comma + values
        comma = ","
        element += 1
        # Commit every 1000 rows
        if element > 999:
            element = 0
            cursor.execute(sql + " " + rows)
            cursor.commit()
            rows = ""
            comma = ""
    # Flush remaining
    if rows != "":
        cursor.execute(sql + " " + rows)
        cursor.commit()


# --------------------------------------------------------
# 3. RAW table 
# --------------------------------------------------------

def create_raw_table(cur):
    print("Reset table - raw_nar")
    cur.execute("DROP TABLE IF EXISTS raw_nar;")
    cur.connection.commit()

    cur.execute("""
        CREATE TABLE raw_nar (
            id_column           INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            LoadTimestamp       DATETIME NOT NULL DEFAULT(GETDATE()),
            SourceFile          VARCHAR(100),
            LOC_GUID            VARCHAR(300),
            ADDR_GUID           VARCHAR(300),
            APT_NO_LABEL        VARCHAR(300),
            CIVIC_NO            VARCHAR(300),
            CIVIC_NO_SUFFIX     VARCHAR(300),
            OFFICIAL_STREET_NAME VARCHAR(300),
            OFFICIAL_STREET_TYPE VARCHAR(300),
            OFFICIAL_STREET_DIR  VARCHAR(300),
            PROV_CODE           VARCHAR(300),
            CSD_ENG_NAME        VARCHAR(300),
            CSD_FRE_NAME        VARCHAR(300),
            CSD_TYPE_ENG_CODE   VARCHAR(300),
            CSD_TYPE_FRE_CODE   VARCHAR(300),
            MAIL_STREET_NAME    VARCHAR(300),
            MAIL_STREET_TYPE    VARCHAR(300),
            MAIL_STEET_DIR      VARCHAR(300),   -- CSV typo
            MAIL_MUN_NAME       VARCHAR(300),
            MAIL_PROV_ABVN      VARCHAR(300),
            MAIL_POSTAL_CODE    VARCHAR(300),
            BG_DLS_LSD          VARCHAR(300),
            BG_DLS_QTR          VARCHAR(300),
            BG_DLS_SCTN         VARCHAR(300),
            BG_DLS_TWNSHP       VARCHAR(300),
            BG_DLS_RNG          VARCHAR(300),
            BG_DLS_MRD          VARCHAR(300),
            BG_X                VARCHAR(300),
            BG_Y                VARCHAR(300),
            BU_N_CIVIC_ADD      VARCHAR(300),
            BU_USE              VARCHAR(300)
        );
    """)
    cur.connection.commit()


def load_raw_from_csv(cur):
    """
    Load all Address_*.csv files into raw_nar.

    Uses latin-1 encoding because some files were not valid utf-8.
    """
    print("Loading table - raw_nar from CSV files")

    folder = r"C:\cst_211_data\NAR"

    address_files = [
        "Address_10.csv",
        "Address_11.csv",
        "Address_12.csv",
        "Address_13.csv",
        "Address_24_part_1.csv",
        "Address_24_part_2.csv",
        "Address_24_part_3.csv",
        "Address_24_part_4.csv",
        "Address_24_part_5.csv",
        "Address_35_part_1.csv",
        "Address_35_part_2.csv",
        "Address_35_part_3.csv",
        "Address_35_part_4.csv",
        "Address_35_part_5.csv",
        "Address_35_part_6.csv",
        "Address_35_part_7.csv",
        "Address_46.csv",
        "Address_47.csv",
        "Address_48_part_1.csv",
        "Address_48_part_2.csv",
        "Address_59_part_1.csv",
        "Address_59_part_2.csv",
        "Address_59_part_3.csv",
        "Address_60.csv",
        "Address_61.csv",
        "Address_62.csv"
    ]

    insert_sql = """
        INSERT INTO raw_nar (
            LoadTimestamp,SourceFile,LOC_GUID, ADDR_GUID, APT_NO_LABEL, CIVIC_NO, CIVIC_NO_SUFFIX,
            OFFICIAL_STREET_NAME, OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR,
            PROV_CODE, CSD_ENG_NAME, CSD_FRE_NAME, CSD_TYPE_ENG_CODE, CSD_TYPE_FRE_CODE,
            MAIL_STREET_NAME, MAIL_STREET_TYPE, MAIL_STEET_DIR,
            MAIL_MUN_NAME, MAIL_PROV_ABVN, MAIL_POSTAL_CODE,
            BG_DLS_LSD, BG_DLS_QTR, BG_DLS_SCTN, BG_DLS_TWNSHP,
            BG_DLS_RNG, BG_DLS_MRD,
            BG_X, BG_Y, BU_N_CIVIC_ADD, BU_USE
        ) VALUES
    """

    for fname in address_files:
        full_path = os.path.join(folder, fname)
        print("  Loading:", full_path)

        # latin-1 to not get UnicodeDecodeError
        with open(full_path, "r", encoding="latin-1") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            data_list = [row for row in reader]

            if data_list:
                pushlist(cur, insert_sql, data_list, fname)

    print("Finished loading raw_nar.")


# --------------------------------------------------------
# 4. Clean 3NF tables
# --------------------------------------------------------

def create_clean_tables(cur):
    print("Reset clean relational tables")

    cur.execute("DROP TABLE IF EXISTS Address;")
    cur.execute("DROP TABLE IF EXISTS Location;")
    cur.execute("DROP TABLE IF EXISTS PostalCode;")
    cur.execute("DROP TABLE IF EXISTS CensusSubdivision;")
    cur.execute("DROP TABLE IF EXISTS BuildingUsage;")
    cur.execute("DROP TABLE IF EXISTS Province;")
    cur.connection.commit()

    # Province lookup
    cur.execute("""
        CREATE TABLE Province (
            ProvinceCode         VARCHAR(10) PRIMARY KEY,
            ProvinceAbbreviation VARCHAR(5)  NOT NULL,
            Description_English  VARCHAR(100) NOT NULL,
            Description_Francais VARCHAR(100) NOT NULL
        );
    """)

    # Building usage lookup
    cur.execute("""
        CREATE TABLE BuildingUsage (
            BU_USE               VARCHAR(10) PRIMARY KEY,
            Description_English  VARCHAR(50) NOT NULL,
            Description_Francais VARCHAR(50) NOT NULL
        );
    """)

    # Census subdivision
    cur.execute("""
        CREATE TABLE CensusSubdivision (
            CSD_ID               INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            CSD_ENG_NAME         VARCHAR(255) NOT NULL,
            CSD_FRE_NAME         VARCHAR(255) NULL,
            CSD_TYPE_ENG_CODE    VARCHAR(10) NULL,
            CSD_TYPE_FRE_CODE    VARCHAR(10) NULL,
            ProvinceCode         VARCHAR(10) NOT NULL
                REFERENCES Province(ProvinceCode)
        );
    """)

    # Postal code (PK is PostalCode itself)
    cur.execute("""
        CREATE TABLE PostalCode (
            PostalCode           VARCHAR(10) PRIMARY KEY,
            ProvinceCode         VARCHAR(10) NOT NULL
                REFERENCES Province(ProvinceCode)
        );
    """)

    # Location (LOC_GUID)
    cur.execute("""
        CREATE TABLE Location (
            LOC_GUID             VARCHAR(36) PRIMARY KEY,
            CSD_ID               INT NOT NULL
                REFERENCES CensusSubdivision(CSD_ID),
            ProvinceCode         VARCHAR(10) NOT NULL
                REFERENCES Province(ProvinceCode),
            BU_USE               VARCHAR(10) NULL
                REFERENCES BuildingUsage(BU_USE),
            BG_DLS_LSD           VARCHAR(10) NULL,
            BG_DLS_QTR           VARCHAR(10) NULL,
            BG_DLS_SCTN          VARCHAR(10) NULL,
            BG_DLS_TWNSHP        VARCHAR(10) NULL,
            BG_DLS_RNG           VARCHAR(10) NULL,
            BG_DLS_MRD           VARCHAR(10) NULL,
            BG_X                 VARCHAR(300) NULL,
            BG_Y                 VARCHAR(300) NULL
        );
    """)

    # Address (ADDR_GUID)
    cur.execute("""
        CREATE TABLE Address (
            ADDR_GUID            VARCHAR(36) PRIMARY KEY,
            LOC_GUID             VARCHAR(36) NOT NULL
                REFERENCES Location(LOC_GUID),
            APT_NO_LABEL         VARCHAR(50)  NULL,
            CIVIC_NO             VARCHAR(50)  NULL,
            CIVIC_NO_SUFFIX      VARCHAR(10)  NULL,
            OFFICIAL_STREET_NAME VARCHAR(255) NULL,
            OFFICIAL_STREET_TYPE VARCHAR(50)  NULL,
            OFFICIAL_STREET_DIR  VARCHAR(50)  NULL,
            MAIL_STREET_NAME     VARCHAR(255) NULL,
            MAIL_STREET_TYPE     VARCHAR(50)  NULL,
            MAIL_STREET_DIR      VARCHAR(50)  NULL,  
            MAIL_MUN_NAME        VARCHAR(255) NULL,
            MAIL_PROV_ABVN       VARCHAR(10)  NULL,
            PostalCode           VARCHAR(10)  NULL
                REFERENCES PostalCode(PostalCode),
            BU_N_CIVIC_ADD       VARCHAR(255) NULL
        );
    """)

    cur.connection.commit()


# --------------------------------------------------------
# 5. Populate reference tables
# --------------------------------------------------------

def populate_province_and_usage(cur):
    print("Populating Province and BuildingUsage")

    cur.execute("""
        INSERT INTO Province (ProvinceCode, ProvinceAbbreviation,
                              Description_English, Description_Francais)
        VALUES
        ('10', 'NL', 'Newfoundland and Labrador', 'Terre-Neuve-et-Labrador'),
        ('11', 'PE', 'Prince Edward Island', 'Île-du-Prince-Édouard'),
        ('12', 'NS', 'Nova Scotia', 'Nouvelle-Écosse'),
        ('13', 'NB', 'New Brunswick', 'Nouveau-Brunswick'),
        ('24', 'QC', 'Quebec', 'Québec'),
        ('35', 'ON', 'Ontario', 'Ontario'),
        ('46', 'MB', 'Manitoba', 'Manitoba'),
        ('47', 'SK', 'Saskatchewan', 'Saskatchewan'),
        ('48', 'AB', 'Alberta', 'Alberta'),
        ('59', 'BC', 'British Columbia', 'Colombie-Britannique'),
        ('60', 'YT', 'Yukon', 'Yukon'),
        ('61', 'NT', 'Northwest Territories', 'Territoires du Nord-Ouest'),
        ('62', 'NU', 'Nunavut', 'Nunavut');
    """)

    cur.execute("""
        INSERT INTO BuildingUsage (BU_USE, Description_English, Description_Francais)
        VALUES
        ('1', 'Residential', 'Résidentiel'),
        ('2', 'Partial Residential', 'Résidentiel partiel'),
        ('3', 'Non Residential', 'Non résidentiel'),
        ('4', 'Unknown', 'Inconnu');
    """)

    cur.connection.commit()


def populate_census_subdivision(cur):
    print("Populating CensusSubdivision from raw_nar")

    cur.execute("""
        INSERT INTO CensusSubdivision (
            CSD_ENG_NAME, CSD_FRE_NAME,
            CSD_TYPE_ENG_CODE, CSD_TYPE_FRE_CODE,
            ProvinceCode
        )
        SELECT DISTINCT
            r.CSD_ENG_NAME,
            r.CSD_FRE_NAME,
            r.CSD_TYPE_ENG_CODE,
            r.CSD_TYPE_FRE_CODE,
            r.PROV_CODE
        FROM raw_nar r
        WHERE r.CSD_ENG_NAME IS NOT NULL
          AND r.PROV_CODE IS NOT NULL;
    """)

    cur.connection.commit()


def populate_postalcode(cur):
    """
    Insert each postal code only once.
    This version already includes the grouped fix to avoid PK errors.
    """
    print("Populating PostalCode from raw_nar (grouped insert)")

    sql_postal = """
        INSERT INTO PostalCode (PostalCode, ProvinceCode)
        SELECT
            p.PostalCode,
            MIN(p.ProvinceCode) AS ProvinceCode
        FROM (
            SELECT 
                LTRIM(RTRIM(MAIL_POSTAL_CODE)) AS PostalCode,
                PROV_CODE AS ProvinceCode
            FROM raw_nar
            WHERE MAIL_POSTAL_CODE IS NOT NULL
              AND LTRIM(RTRIM(MAIL_POSTAL_CODE)) <> ''
        ) AS p
        GROUP BY p.PostalCode;
    """

    cur.execute(sql_postal)
    cur.connection.commit()


# --------------------------------------------------------
# 6. Populate Location and Address 
# --------------------------------------------------------

def populate_location(cur):
    print("Populating Location from raw_nar (grouped by LOC_GUID)...")

    sql = """
        INSERT INTO Location (
            LOC_GUID, CSD_ID, ProvinceCode, BU_USE,
            BG_DLS_LSD, BG_DLS_QTR, BG_DLS_SCTN, BG_DLS_TWNSHP,
            BG_DLS_RNG, BG_DLS_MRD,
            BG_X, BG_Y
        )
        SELECT
            r.LOC_GUID,
            MIN(cs.CSD_ID)      AS CSD_ID,
            MIN(r.PROV_CODE)    AS ProvinceCode,
            MIN(r.BU_USE)       AS BU_USE,
            MIN(r.BG_DLS_LSD)   AS BG_DLS_LSD,
            MIN(r.BG_DLS_QTR)   AS BG_DLS_QTR,
            MIN(r.BG_DLS_SCTN)  AS BG_DLS_SCTN,
            MIN(r.BG_DLS_TWNSHP) AS BG_DLS_TWNSHP,
            MIN(r.BG_DLS_RNG)   AS BG_DLS_RNG,
            MIN(r.BG_DLS_MRD)   AS BG_DLS_MRD,
            MIN(r.BG_X)         AS BG_X,
            MIN(r.BG_Y)         AS BG_Y
        FROM raw_nar r
        JOIN CensusSubdivision cs
          ON cs.CSD_ENG_NAME = r.CSD_ENG_NAME
         AND cs.ProvinceCode = r.PROV_CODE
        WHERE r.LOC_GUID IS NOT NULL
        GROUP BY r.LOC_GUID;
    """

    cur.execute(sql)
    cur.connection.commit()


def populate_address(cur):
    print("Populating Address from raw_nar (joining to PostalCode)...")

    sql = """
        INSERT INTO Address (
            ADDR_GUID, LOC_GUID,
            APT_NO_LABEL, CIVIC_NO, CIVIC_NO_SUFFIX,
            OFFICIAL_STREET_NAME, OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR,
            MAIL_STREET_NAME, MAIL_STREET_TYPE, MAIL_STREET_DIR,
            MAIL_MUN_NAME, MAIL_PROV_ABVN,
            PostalCode, BU_N_CIVIC_ADD
        )
        SELECT
            r.ADDR_GUID,
            r.LOC_GUID,
            r.APT_NO_LABEL,
            r.CIVIC_NO,
            r.CIVIC_NO_SUFFIX,
            r.OFFICIAL_STREET_NAME,
            r.OFFICIAL_STREET_TYPE,
            r.OFFICIAL_STREET_DIR,
            r.MAIL_STREET_NAME,
            r.MAIL_STREET_TYPE,
            r.MAIL_STEET_DIR,   -- CSV typo mapped into MAIL_STREET_DIR
            r.MAIL_MUN_NAME,
            r.MAIL_PROV_ABVN,
            pc.PostalCode,      -- only postal codes that exist in PostalCode table
            r.BU_N_CIVIC_ADD
        FROM raw_nar r
        LEFT JOIN PostalCode pc
          ON LTRIM(RTRIM(r.MAIL_POSTAL_CODE)) = pc.PostalCode
        WHERE r.ADDR_GUID IS NOT NULL;
    """

    cur.execute(sql)
    cur.connection.commit()


# --------------------------------------------------------
# 7. Main 
# --------------------------------------------------------

def main():
    conn = get_connection()
    cur = conn.cursor()

    
    # Uncomment the functions below before running - to avoid accidentl deletion of tables
    #create_raw_table(cur)
    #load_raw_from_csv(cur)
    #create_clean_tables(cur)
    #populate_province_and_usage(cur)
    #populate_census_subdivision(cur)
    #populate_postalcode(cur)

    
    print("Rebuilding Location table from raw_nar...")
    #populate_location(cur)

    print("Rebuilding Address table from raw_nar...")
    #populate_address(cur)

    cur.close()
    conn.close()
    print("GA5 NAR ETL complete.")


if __name__ == "__main__":
    main()
