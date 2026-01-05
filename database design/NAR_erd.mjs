---
title: National Address Register
---

erDiagram
  Province {
    string ProvinceCode PK
    string ProvinceAbbreviation
    string Description_English
    string Description_Francais
  }

  BuildingUsage {
    string BU_USE PK
    string Description_English
    string Description_Francais
  }

  CensusSubdivision {
    int CSD_ID PK
    string CSD_ENG_NAME
    string CSD_FRE_NAME
    string CSD_TYPE_ENG_CODE
    string CSD_TYPE_FRE_CODE
    string ProvinceCode FK
  }

  PostalCode {
    string PostalCode PK
    string ProvinceCode FK
  }

  Location {
    string LOC_GUID PK
    int CSD_ID FK
    string ProvinceCode FK
    string BU_USE FK
    string BG_DLS_LSD
    string BG_DLS_QTR
    string BG_DLS_SCTN
    string BG_DLS_TWNSHP
    string BG_DLS_RNG
    string BG_DLS_MRD
    string BG_X
    string BG_Y
  }

  Address {
    string ADDR_GUID PK
    string LOC_GUID FK
    string APT_NO_LABEL
    string CIVIC_NO
    string CIVIC_NO_SUFFIX
    string OFFICIAL_STREET_NAME
    string OFFICIAL_STREET_TYPE
    string OFFICIAL_STREET_DIR
    string MAIL_STREET_NAME
    string MAIL_STREET_TYPE
    string MAIL_STREET_DIR
    string MAIL_MUN_NAME
    string MAIL_PROV_ABVN
    string PostalCode FK
    string BU_N_CIVIC_ADD
  }

  Province ||--o{ CensusSubdivision : "has"
  Province ||--o{ PostalCode : "has"
  Province ||--o{ Location : "contains"
  BuildingUsage ||--o{ Location : "usage"
  CensusSubdivision ||--o{ Location : "has"
  Location ||--o{ Address : "has"
  PostalCode ||--o{ Address : "used by"

