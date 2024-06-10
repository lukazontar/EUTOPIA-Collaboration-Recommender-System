# :world_map: Topic Modeling

## :books: Introduction

This document provides an overview of the processes involved in embedding research topics and articles, and deriving the
most probable research topics for each article based on their embeddings. The scripts interact with the CERIF (Common
European Research Information Format) registry, which is a standard for managing and exchanging research information in
a structured manner.

## :classical_building: CERIF Registry

The CERIF (Common European Research Information Format) registry is a standard for managing and exchanging research
information. It provides a structured format for representing various research entities such as projects, publications,
patents, and organizational units. CERIF ensures interoperability and enhances the accessibility of research information
across different systems and institutions.

## :robot: Embedding Model

We used `scibert-scivocab-cased` model for embedding research topics and articles. This model was chosen because it is
specifically trained on scientific text and has a large vocabulary size.

## :brain: Embedding Research Topics

### :newspaper: Top N Article Embeddings

**Data table:** `ANALYTICS.TEXT_EMBEDDING_CERIF_RESEARCH_TOPIC_TOP_N_ARTICLES`.

First, we read through the CERIF research topics and fetch the top N articles querying topic names via  *Crossref API*.
Embeddings of top N articles are then combined and stored in a BigQuery table

### :scroll: Topic Metadata Embeddings

**Data table:** `ANALYTICS.TEXT_EMBEDDING_CERIF_RESEARCH_TOPIC_METADATA`.

After embedding the research topic articles, we embed the metadata of research topics using a transformer model. The
metadata of a research topic includes the topic branch and subbranch code from CERIF and its name.

### :heavy_plus_sign: Research Topic Embeddings

**Data table:** `ANALYTICS.TEXT_EMBEDDING_RESEARCH_TOPIC`.

We combine the embeddings of research topic articles and metadata to derive the final research topic embeddings, which
is calculated as a weighted sum of the embeddings.

## :page_facing_up: Embedding Articles

**Data table:** `ANALYTICS.TEXT_EMBEDDING_ARTICLE`.

In parallel to embedding research topics, we also embed all the articles of EUTOPIA institutions using the same
transformer model.

## :mag: Deriving Research Topics

**Data table:** `ANALYTICS.ARTICLE_TOPIC`.

This script compares the embeddings of articles and research topics using a cosine similarity metric to derive the top 3
most probable research topics for each article.
