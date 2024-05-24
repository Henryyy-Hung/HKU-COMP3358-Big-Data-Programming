# Big Data Processing with Hadoop and Spark

This project demonstrates the use of Apache Hadoop and Apache Spark to handle and analyze big data, specifically user query logs from a search engine. We preprocess the data using Hadoop and then perform analysis using Spark.

## Project Structure

- **Hadoop Preprocessing**: 
  - Strips down the URLs in the query logs to their base form.
- **Spark Analysis**:
  - Tokenizes the URLs and ranks the most frequent tokens.
  - Analyzes the query logs to find the time periods with the most queries.
