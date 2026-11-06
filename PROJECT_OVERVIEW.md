# FMCG Data Engineering Project

## What this project does

This project demonstrates a Databricks data pipeline that combines sales data from a parent FMCG company and a child company. It cleans and integrates the data, maintains monthly sales summaries as new orders arrive, and prepares an enriched dataset for dashboard analysis.

The notebooks use Spark/PySpark and Delta Lake, organized into a medallion-style structure:

- **Bronze:** source rows plus ingestion/file metadata.
- **Silver:** cleaned and standardized records, including product/customer matching.
- **Gold:** business dimensions and order facts for reporting.

The intended catalog is `fmcg`, with `bronze`, `silver`, and `gold` schemas.

## Dataset at a glance

The repository includes local CSV samples. Counts below are data rows (excluding headers):

| Source | Data included |
|---|---|
| Parent company, full load | Customers (18), products (397), gross prices (794), and orders (93,055). |
| Parent company, incremental | 4,485 order rows. |
| Child company, full load | Customers (39), products (20), gross prices (108), and 51,810 order rows across 151 files. |
| Child company, incremental | 9,967 order rows across 31 daily files for December 2025. |

The order files contain order ID, order date, customer ID, product ID, and quantity. Customer files provide customer identifiers and descriptive details; product files provide product identifiers, names, and categories; gross-price files provide product prices over time. Some child-company sample records contain duplicate IDs or inconsistent spelling/spacing, which the transformation notebooks address.

The notebooks reference configured Databricks/cloud-storage paths (including `s3://sportsbar-final/` in the order notebooks); they do not automatically read the repository's local CSV paths.

## What happens to the data

1. **Set up tables and dimensions.** Notebooks create the catalog/schemas and build customer, product, price, and date dimensions. They standardize fields, remove duplicates, parse dates/prices, derive product attributes/codes, and merge child-company dimensions into parent Gold dimensions.
2. **Load orders.** The full-load workflow ingests the initial landing files. The incremental workflow handles newly arrived files through staging tables, then moves input files to processed storage.
3. **Clean and integrate orders.** The pipeline normalizes dates and IDs, removes duplicates, joins orders to products to obtain product codes, and updates Silver and child-company Gold data.
4. **Refresh parent sales totals.** Order quantities are aggregated at **month start × product × customer**. For incremental batches, the workflow recalculates complete totals for affected months and merges them into the parent Gold fact table.
5. **Support dashboard analysis.** A SQL view joins monthly facts to date, customer, product, and price dimensions. It derives revenue as quantity sold multiplied by price; the dashboard presents sales KPIs, trends, rankings, and channel/product breakdowns.

The Databricks job shown in the project sequences customer, product, and price processing before order processing. The dashboard query and job details are described in [the dashboard assets](2_dashboarding/) and [the notebooks](1_codes/).

## Where to look

- [Sample data](0_data/) — parent and child company CSV inputs.
- [Databricks notebooks](1_codes/) — setup, dimension transformations, and full/incremental order processing.
- [Dashboard query and export](2_dashboarding/) — enriched view SQL and dashboard output.
- [Architecture image](resources/project_architecture.png) — visual summary of the pipeline.
