# Reproducers

## Reproducer for bug in Azure Cosmos SDK for Java

Change feed may skip processing of some records when the database has exhausted RUs, and many requests are throttled (error 429).