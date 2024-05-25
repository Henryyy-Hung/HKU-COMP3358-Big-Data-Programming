# Big Data Processing with Hadoop and Spark

This project demonstrates the use of Apache Hadoop and Apache Spark to handle and analyze big data, specifically user query logs from a search engine. We preprocess the data using Hadoop and then perform analysis using Spark.

## Project Structure

- **Hadoop Preprocessing**: 
  - Strips down the URLs in the query logs to their base form.
- **Spark Analysis**:
  - Tokenizes the URLs and ranks the most frequent tokens.
  - Analyzes the query logs to find the time periods with the most queries.

## Workflow

1. **Data Preprocessing with Hadoop**:
  - Input: Raw search engine query logs.
  - Process: Each log entry is parsed to extract and format the timestamp, and to simplify the URL to its domain name.
  - Output: A cleaner version of the log with formatted timestamps and domains, along with a count of how many times each domain was queried in each time slot.

2. **Data Analysis with Spark**:
  - Input: The output from the Hadoop job.
  - Process A (Token Ranking): Tokenizes the domains and counts the frequency of each token, sorting them to find the most common tokens.
  - Process B (Query Ranking By Time): Aggregates data by time slots to count the number of queries, identifying the busiest periods.
  - Output: Insights into the most common tokens and the peak query times.
