# Analytics Plan

This document outlines the analytics plan for the collaboration recommender system. It is divided into two main parts,
the exploratory data analysis and the collaboration characteristics.
The idea is that these insights will power an interactive analytics dashboard that will help us understand the
collaboration dynamics between EUTOPIA institutions.

<hr/>

## :detective: Exporatory Data Analysis

First of all, we need to have a good understanding of the data we have including data coverage, quality and some basic
statistics.

### :bar_chart: Basic Statistics

Regarding understanding the basic statistics of article collaboration we need to be able to answer questions:

- How many articles do we have?
- How many authors do we have?
- How is the number of articles and authors trending over time?
- How many articles and authors are from EUTOPIA institutions?
- How many articles were made in collaboration internally vs. between different institutions, and how many with only one
  author?
- How many articles were made in collaboration between EUTOPIA institutions?
- How many new collaboration events do we have?
- Which authors tends to be more collaborative with EUTOPIA institutions?

### :shield: Data Coverage

Going forward, we need to understand data coverage on most important fields:

- How many articles has titles, abstract, references?
- How many authors has affiliation information?
- How many authors has ORCID ID?
- How many articles are written in English vs. other languages?
- How many articles are open access, and we can access the full text via Unpaywall?
- ...

<hr/>

## :people_holding_hands: Collaboration Characteristics

After understanding the basic statistics of the data, we want to dive deeper into collaboration characteristics.
Primarily, we want to understand two things:

1. **Occurrence of new collaborations:** When and why does a new collaboration occur?
2. **Influence of new collaborations:** How does a new collaboration affect all parties involved?

### :new: Occurrence of New Collaborations

**Questions:**

1. Does the initiation of a new collaboration stem from expertise, meaning, is it driven by one institution possessing
   significant proficiency in a particular area and thus being sought after for their complementary skills?
2. Can we connect a new collaboration event to a topic trend peak?
3. Are collaborations more likely to be initiated by early-career researchers or established academics? (probably not
   possible to confirm with current data)
2. How does affiliation change over time? Can we detect people moving to different institutions vs. collaborating? (
   probably not possible to confirm with current data)

**Hypotheses:**

1. Collaboration is often motivated by expertise.
2. Collaboration can be a result of a topic trend peak and the surrounding hype.
3. New collaborations are more likely to be initiated by established academics with a large network of collaborators and
   resources.

### :chart_with_upwards_trend: Influence of New Collaborations

**Questions:**

1. How does the trend of collaborations increase after the first collaboration?
2. How does a new collaboration affect the overall research direction of all involved institutions/working groups?
3. How does success of a new collaboration affect the likelihood of future collaborations?

**Hypotheses:**

1. Collaboration trend increases exponentially after the first collaboration.
2. Collaboration significantly affects the research direction of all involved institutions/departments.
3. If a new collaboration is successful, the likelihood of future collaborations increases.

<hr/>

## :computer: Analytics Dashboard

In this section, we outline a rough structure for the analytics dashboard with which we will be able to interactively
explore the collaboration dynamics between EUTOPIA institutions and authors.

1. **Overview dashboard page:** shows some basic statistics like the number of articles, authors, trends, EUTOPIA
   collaboration rate, and new collaboration rate.
    - This page will include all the KPIs that will show the current state of the collaboration dynamics and our data.
2. **Author collaboration page:** shows the collaboration characteristics but from the perspective of a single author.
   We will present the author's collaboration history, the impact of collaborations and try to reason why a new
   collaboration occurred from the author's perspective.
3. **Collaboration characteristics page:** shows the occurrence and influence of new collaborations.
