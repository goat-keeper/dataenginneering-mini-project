# Prompt for Generating the FMCG Pipeline Architecture Design

Copy and paste this prompt into an AI diagram or design tool. Attach the project notebooks and, if supported, the Databricks job screenshot as reference material.

```text
Create a clear, technically accurate data architecture diagram for an academic project titled “FMCG Incremental Data Engineering Pipeline and Analytics Dashboard.” Use a clean professional data engineering style, a wide landscape canvas, readable labels, consistent arrows, and a restrained palette. The design should be suitable for insertion into a project report. Do not invent cloud services or components that are not listed below.

Show these components and flows:

1. Source data on the left:
   - Parent company CSV data: orders, customers, products, and gross prices.
   - Child company CSV data: customers, products, gross prices, and orders.
   - Child order files have a full-load landing path and a recurring incremental landing path.

2. Databricks and Delta Lake in the central processing area:
   - Unity Catalog catalog `fmcg` with schemas `bronze`, `silver`, and `gold`.
   - Dimension processing tasks: customer processing, product processing, and price processing.
   - Bronze stores ingested records and source metadata such as read timestamp, file name, and file size.
   - Silver performs data cleaning and conformance: duplicate removal, text normalization, date parsing, price validation, product/customer matching, and product code derivation.
   - Gold child staging dimensions: `sb_dim_customers`, `sb_dim_products`, and `sb_dim_gross_price`.
   - Delta MERGE operations integrate child dimensions into parent Gold dimensions: `dim_customers`, `dim_products`, and `dim_gross_price`.

3. Order processing:
   - Full-load path reads order files from landing, appends raw data to Bronze, cleans and joins orders to products, writes child Gold order facts, aggregates quantity by month start + product code + customer code, and merges the result into parent Gold `fact_orders`.
   - Incremental path reads new landing files, stores the batch in Bronze and staging tables, moves input files to processed storage, cleans and joins orders, merges the batch into Silver and child Gold `sb_fact_orders`, identifies affected months, recalculates the complete monthly aggregates for those months, and merges those aggregates into parent Gold `fact_orders`.
   - Show the aggregation grain explicitly as month + product + customer.

4. Databricks job orchestration:
   - Label the job “FMCG Incremental update.”
   - Show four tasks in order: `dim_processing_customers` → `dim_processing_products` → `dim_processing_prices` → `fact_processing_orders`.
   - Indicate that the task graph is orchestrated by a Databricks job. Do not imply a schedule or trigger unless one is explicitly provided.

5. Analytics on the right:
   - Show an enriched Gold analytical view joining monthly order facts to the date, customer, product, and gross price dimensions.
   - Show the dashboard consuming this view, with Year, Quarter, Month, Channel, and Category filters.
   - Dashboard cards/charts: total revenue, total quantity sold, unique customers, average selling price, top products by revenue, revenue share by channel, monthly revenue trend, customer revenue ranking, top variant revenue, and product price versus quantity.

Use directional arrows to show data movement and label important operations such as “clean & conform,” “Delta MERGE,” “affected-month recalculation,” and “dashboard query.” Visually distinguish source files, Delta tables, transformation notebooks, orchestration, and dashboard outputs. Keep all text legible, avoid crossing arrows, and make the full pipeline understandable without explanatory prose.

Include a small note: “Monthly parent fact grain: month start × product × customer.”

Do not include fabricated KPIs, performance numbers, security claims, schedules, or unsupported components such as Auto Loader, DLT/Lakeflow, Kafka, Airflow, or Power BI. Output one polished architecture diagram. If the tool supports editable output, provide editable diagram elements and a high-resolution PNG or SVG export.
```

## Optional Style Direction

If the diagram tool supports visual styling instructions, append:

```text
Style: academic report figure, white or very light background, dark navy headings, teal accents for Delta Lake layers, muted blue for source systems, and a contrasting but accessible color for the dashboard. Use simple icons sparingly. Prefer grouped containers and short labels over decorative illustrations. Add a concise title and a figure subtitle describing the end-to-end FMCG data flow.
```
