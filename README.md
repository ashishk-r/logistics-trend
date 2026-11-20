# Transportation and Logistics Trend Analysis

**College project using Scala, Apache Spark, and Streamlit**

This project studies supply-chain delivery performance with the DataCo Smart Supply Chain dataset. A Scala/Spark batch job cleans the order-level data and creates small aggregate files. A Streamlit dashboard reads those aggregates and lets a viewer explore trends by month, market, region, and shipping mode.

## What the project answers

- How does the late-delivery rate change over time?
- Which shipping modes and regions have higher late-delivery rates?
- How do actual shipping days compare with scheduled shipping days?
- How much of the dataset consists of cancelled orders?

The unit of analysis is a **unique order**. The source file contains order-item rows, so the Spark job groups rows by `Order Id` before counting orders. It excludes cancelled orders from delivery-rate and shipping-time averages, while reporting cancellations separately.

## Data source

Download `DataCoSupplyChainDataset.csv` from the [DataCo SMART SUPPLY CHAIN dataset on Mendeley Data](https://data.mendeley.com/datasets/8gx2fvg2k6/5). The dataset page describes its supply-chain use and lists a CC BY 4.0 license. Include attribution to Fabian Constante, Fernando Silva, and António Pereira in your submission.

Put the downloaded CSV here:

```text
data/raw/DataCoSupplyChainDataset.csv
```

The Spark job only selects order dates, shipping dates, delivery status, shipping duration, shipping mode, market, and region. It does not export customer names, emails, addresses, or other customer-level fields.

## Project layout

```text
build.sbt
src/main/scala/TransportationTrendAnalysis.scala
data/raw/                         # downloaded source CSV (keep out of GitHub)
dashboard/streamlit_app.py        # dashboard entry point
dashboard/requirements.txt
dashboard/data/                   # generated Spark summaries
docs/project_report.md            # report draft
```

## Run the Spark analysis

Prerequisites: Java 17, sbt, and Apache Spark 3.5.6. The Spark distribution provides the `spark-submit` command. On macOS, you can download the matching pre-built Spark release into Downloads:

```bash
cd ~/Downloads
curl -fLO https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
tar -xzf spark-3.5.6-bin-hadoop3.tgz
```

From the project folder, build the Scala application:

```bash
sbt package
```

Run it locally with Spark:

```bash
~/Downloads/spark-3.5.6-bin-hadoop3/bin/spark-submit --master 'local[*]' \
  --class TransportationTrendAnalysis \
  target/scala-2.12/transportation-logistics-trends_2.12-0.1.0.jar \
  data/raw/DataCoSupplyChainDataset.csv \
  dashboard/data
```

The output path will contain these folders, each with a Spark-generated CSV file:

- `overall_kpis/`
- `monthly_trends/`
- `shipping_mode_summary/`
- `region_summary/`
- `segment_monthly/`
- `status_summary/`

## Run the Streamlit dashboard locally

```bash
python3 -m venv .venv
./.venv/bin/python -m pip install -r dashboard/requirements.txt
./.venv/bin/python -m streamlit run dashboard/streamlit_app.py
```

The dashboard uses precomputed summaries so it does not need to start Spark or load the full source CSV. Generate the summaries first.

## Deploy to Streamlit Community Cloud

1. Generate the Spark summaries locally.
2. Push this project to a GitHub repository. Include `dashboard/data/` summary CSVs so the hosted app has data to display. Keep the original dataset out of the repository; `data/raw/` is ignored by Git.
3. Sign in to [Streamlit Community Cloud](https://share.streamlit.io/) with GitHub and create an app from the repository.
4. Set the app file path to `dashboard/streamlit_app.py`. The dependency file is alongside it at `dashboard/requirements.txt`.

Streamlit Community Cloud deploys from GitHub and reads dependencies from a `requirements.txt` file. You need permission to deploy the selected repository. See Streamlit's [deployment guide](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/deploy) and [dependency guide](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/app-dependencies).

## Troubleshooting on macOS

- If Spark stops with `UnsupportedOperationException: getSubject is not supported`, check `java -version`. Spark 3.5.6 supports Java 8, 11, and 17. Select or install JDK 17, then set it for the current terminal with `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` and `export PATH="$JAVA_HOME/bin:$PATH"` before running Spark.
- If `find` prints no generated files, use `find dashboard/data -name 'part-*.csv'` (with no backslash before `*`) and confirm the Spark job completed successfully.
- If the shell says `python: command not found`, use the project's interpreter directly: `./.venv/bin/python -m streamlit run dashboard/streamlit_app.py`.

## Interpretation and limitations

This is descriptive analysis of one historical supply-chain dataset. The source is not a live feed, and the results should not be presented as current performance for a real company. Delivery-risk/status fields are used to summarize observed outcomes; this project does not train a predictive model. The dataset contains customer-level columns, but this project avoids using or publishing them.

## Attribution

Data: Constante, Fabian; Silva, Fernando; Pereira, António. *DataCo SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS*. Mendeley Data, version 5, 2019. DOI: [10.17632/8gx2fvg2k6.5](https://doi.org/10.17632/8gx2fvg2k6.5). CC BY 4.0.
