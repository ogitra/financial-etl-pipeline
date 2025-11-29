/*
    QUERY USED IN THE "EXTRACT" STAGE OF THE ETL PIPELINE
    -----------------------------------------------------------------------
    • This SQL demonstrates how to extract accounting data from multiple tables,
      standardize monetary values, prioritize consolidated statements, and
      produce a clean dataset for further processing in Python.

    • IMPORTANT:
      All table names, column names, account codes and sector names
      have been modified for portfolio and educational purposes.


    MAIN TOPICS COVERED:
    -------------------------------
    ✔ Monetary value standardization (e.g., converting millions/thousands)
    ✔ Selecting only ANNUAL financial statements
    ✔ Filtering relevant financial accounts
    ✔ Deduplication using ROW_NUMBER prioritizing CONSOLIDATED statements
    ✔ Use of multiple CTEs for clarity and organization
    ✔ Normalization of the final company name (trade name vs legal name)
    ✔ JOINing financial data, companies, and chart of accounts
*/


/* ============================================================================
   1) CTE raw_financials
      - Loads raw financial data
      - Standardizes values based on monetary unit
      - Applies ROW_NUMBER to prioritize CONSOLIDATED > INDIVIDUAL
   ============================================================================ */
WITH raw_financials AS (
    SELECT
        f.CompanyId,
        f.ClosingDate,
        fc.AccountId,

        /* Monetary standardization:
           If the unit is "MILLION" → multiply by 1,000,000
           If the unit is "THOUSAND" → multiply by 1,000
           Otherwise → keep the original value */
        CASE
            WHEN f.MonetaryUnit = 'MILLION' THEN fc.Value * 1000000
            WHEN f.MonetaryUnit = 'THOUSAND' THEN fc.Value * 1000
            ELSE fc.Value
        END AS StandardizedValue,

        /* ROW_NUMBER priority:
           CONSOLIDATED → priority 1
           INDIVIDUAL  → priority 2
           OTHERS      → priority 3
           Ensures that the consolidated version is selected later. */
        ROW_NUMBER() OVER (
            PARTITION BY
                f.CompanyId,
                f.ClosingDate,
                fc.AccountId
            ORDER BY
                CASE
                    WHEN f.StatementType = 'CONSOLIDATED' THEN 1
                    WHEN f.StatementType = 'INDIVIDUAL'  THEN 2
                    ELSE 3
                END
        ) AS rn

    FROM FinancialStatement f
    JOIN FinancialStatementAccount fc
        ON fc.StatementId = f.StatementId
    WHERE
        f.PeriodType = 'ANNUAL'                                   -- Select only annual statements
        AND f.ClosingDate BETWEEN '2020-12-31' AND '2024-12-31'   -- Date filter
        AND fc.AccountId IN (                                     -- Relevant account IDs
            55, 56, 57, 73, 103,
            178, 230, 286,
            332, 333, 335, 347, 375,
            421, 462, 473
        )
        AND fc.Value IS NOT NULL
        AND fc.Value <> 0
),


/* ============================================================================
   2) CTE financials_filtered
      - Removes duplicates, keeping only RN = 1
      - Ensures we keep the consolidated version whenever available
   ============================================================================ */
financials_filtered AS (
    SELECT
        CompanyId,
        ClosingDate,
        AccountId,
        StandardizedValue
    FROM raw_financials
    WHERE rn = 1
),


/* ============================================================================
   3) CTE companies
      - Normalizes company name: TradeName > LegalName
      - Trims left/right whitespace
      - Filters only companies from a specific sector
   ============================================================================ */
companies AS (
    SELECT DISTINCT
        c.CompanyId,
        CASE
            WHEN NULLIF(LTRIM(RTRIM(c.TradeName)), '') IS NOT NULL
                THEN LTRIM(RTRIM(c.TradeName))
            ELSE LTRIM(RTRIM(c.LegalName))
        END AS FinalCompanyName
    FROM Company c
    JOIN CompanySector cs
        ON c.CompanyId = cs.CompanyId
    JOIN Sector s
        ON cs.SectorId = s.SectorId
    WHERE
        s.SectorName = 'Retail Sector'   -- fictitious name
)


/* ============================================================================
   4) FINAL SELECT
      - Joins financials + companies + chart of accounts
   ============================================================================ */
SELECT
    f.CompanyId,
    c.FinalCompanyName,
    f.ClosingDate,
    f.AccountId,
    ca.AccountCode,
    ca.AccountDescription,
    f.StandardizedValue
FROM financials_filtered f
JOIN companies c
    ON c.CompanyId = f.CompanyId
JOIN ChartOfAccounts ca
    ON ca.AccountId = f.AccountId
ORDER BY
    c.FinalCompanyName,
    f.ClosingDate DESC,
    f.AccountId;
