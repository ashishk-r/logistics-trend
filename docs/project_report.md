# Transportation and Logistics Trend Analysis Using Scala and Spark

## Abstract

This project analyzes historical supply-chain delivery performance using the DataCo Smart Supply Chain dataset. Apache Spark, through its Scala API, is used to clean the data, consolidate item rows into unique orders, and compute delivery trends. A Streamlit dashboard presents the aggregated results by month, market, region, and shipping mode. The analysis focuses on delivery completion, late-delivery rates, and actual versus scheduled shipping time. It is descriptive: it summarizes recorded outcomes and does not predict future delays.

> Replace this note with a short summary of the results after running the Spark job and reviewing the dashboard. Do not invent findings before processing the downloaded dataset.

## 1. Introduction

Transportation and logistics operations depend on coordinating shipments, shipping methods, delivery schedules, and regional networks. Historical order data can help reveal when late deliveries are more common and where shipping performance differs. This project uses distributed data processing with Scala and Apache Spark to prepare these measurements, then presents them in an interactive dashboard.

## 2. Problem statement

The dataset contains order and shipping information at item level. It is difficult to compare shipping performance by time, geography, and shipping mode without first cleaning the data and establishing a consistent unit of analysis. This project consolidates records to unique orders and summarizes delivery outcomes using consistent metrics.

## 3. Objectives

1. Load and process the DataCo supply-chain CSV using Scala and Apache Spark.
2. Convert item-level records into one record per order to avoid overcounting.
3. Compare delivery performance over time, across regions and markets, and between shipping modes.
4. Compare actual shipping duration with scheduled duration and report cancellations separately.
5. Present the aggregate results through a Streamlit dashboard suitable for interactive exploration.

## 4. Research questions

- How does the late-delivery rate vary by month?
- Which regions and markets have the highest late-delivery rate?
- How do recorded late-delivery rates vary by shipping mode?
- How do actual shipping days compare with scheduled shipping days?
- What share of orders is cancelled?

## 5. Dataset

The project uses `DataCoSupplyChainDataset.csv` from the [DataCo SMART SUPPLY CHAIN dataset on Mendeley Data](https://data.mendeley.com/datasets/8gx2fvg2k6/5), version 5. The dataset is published under CC BY 4.0. It includes delivery status, actual and scheduled shipping days, shipping mode, order date, market, and order region. The source also contains customer-level fields; this project does not use those fields or publish row-level records.

The source is historical and is not a live logistics feed. The unit of analysis is a unique order, created by grouping all item rows with the same `Order Id`.

## 6. Tools and technologies

- **Scala** for the analysis application.
- **Apache Spark SQL** for distributed CSV processing and aggregation.
- **sbt** for building the Scala application.
- **Streamlit** for the interactive web dashboard.
- **Pandas** to load the small summary CSVs in the dashboard.
- **Streamlit Community Cloud** for public deployment from GitHub.

## 7. Methodology

1. Read the source CSV with its header and infer column types.
2. Keep only the fields needed for dates, delivery status, shipping duration, shipping mode, market, and region.
3. Group item rows by order ID and retain one set of order-level values.
4. Identify cancelled orders from delivery status. Report them separately and exclude them from completed-order delivery-rate and shipping-time averages.
5. Calculate late-delivery rate from the recorded delivery status/risk indicator; calculate excess shipping days as the positive part of actual days minus scheduled days.
6. Aggregate metrics overall and by month, shipping mode, region, and market/region/shipping-mode segments.
7. Write compact CSV summaries for Streamlit to read.
8. Use dashboard filters and charts to explore the generated summaries.

### Main metrics

| Metric | Definition |
|---|---|
| Completed orders | Unique orders whose status is not cancelled |
| Late orders | Completed unique orders recorded as late |
| Late-delivery rate | Late orders divided by completed orders, multiplied by 100 |
| Average actual shipping days | Mean actual shipping duration for completed orders |
| Average scheduled shipping days | Mean scheduled duration for completed orders |
| Average excess days per order | Mean of `max(actual days − scheduled days, 0)` for completed orders |

## 8. System design

```text
DataCo source CSV
       │
       ▼
Scala + Apache Spark
  clean → deduplicate by order → aggregate
       │
       ▼
Small summary CSV folders
       │
       ▼
Streamlit dashboard
  filters → KPI cards → charts → tables
```

Spark runs as a batch job before deployment. The hosted Streamlit app reads the precomputed summary files, so it does not need to process the full source dataset each time a viewer opens the dashboard.

## 9. Results

The Spark job consolidated order-item rows into **65,752 unique orders**, covering order dates from January 2015 to January 2018. Of these, **62,897 were completed** and **2,855 were cancelled**. Among completed orders, **36,048 were marked late**, giving a late-delivery rate of **57.31%**. Average actual shipping time was **3.50 days**, compared with **2.94 scheduled days**. The mean positive excess time across all completed orders was **0.93 days per order**.

Monthly late-delivery rates ranged from **55.04% in April 2017** to **59.86% in June 2016**. The month-to-month range is relatively narrow in this dataset, though it does not establish why late rates changed.

Rates differed by shipping mode: First Class was **100.00%** (9,602 orders), Second Class **79.99%** (12,256), Same Day **48.37%** (3,407), and Standard Class **39.85%** (37,632). These are descriptive comparisons. Shipping modes also have different scheduled delivery windows, so the rates should not be interpreted as evidence that a mode itself caused delays.

Among regions, Central Africa had the highest late rate (**60.04%**, 533 orders) and Canada the lowest (**52.58%**, 291 orders). Western Europe had the largest order count (**9,563**) and a **58.40%** late rate. The smaller regional counts make those regional extremes less stable than the larger aggregates.

## 10. Conclusion and recommendations

In this historical dataset, more than half of completed orders were recorded as late, while average actual shipping time exceeded the scheduled time. Rates differed substantially across shipping modes, with First Class and Second Class highest. Operations teams could review the delivery promises and processes associated with those categories and compare them with Standard Class. Regional outliers should be reviewed alongside their order counts. These are investigation priorities, not causal conclusions; the data does not explain why a delay occurred.

## 11. Limitations

- The data is historical, not real-time.
- The results depend on the completeness and accuracy of the source fields.
- The source is item-level; grouping by order ID avoids counting a multi-item order several times, but assumes order-level shipping attributes are consistent across its rows.
- Descriptive comparisons do not establish why a delay occurred and do not predict future delivery outcomes.
- The project does not use personally identifying customer fields.

## References

1. Constante, F., Silva, F., & Pereira, A. (2019). *DataCo SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS* (Version 5). Mendeley Data. https://doi.org/10.17632/8gx2fvg2k6.5
2. Apache Spark. *Spark SQL, DataFrames and Datasets Guide*. https://spark.apache.org/docs/3.5.6/sql-programming-guide.html
3. Streamlit. *Deploy your app on Community Cloud*. https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/deploy
