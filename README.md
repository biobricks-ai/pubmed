# Pubmed

## DESCRIPTION
```json
namespace: Pubmed
description: Parquet tables transforming pubmed articles into json documents
dependencies: 
  - name: pubmed
    url: ftp://ftp.ncbi.nlm.nih.gov/pubmed
```

# Description

> PubMed® comprises more than 34 million citations for biomedical literature - [Pubmed](https://pubmed.ncbi.nlm.nih.gov/).

This brick converts the xml pubmed repository into parquet files linking pubmed ids to json documents.

- The data was downloaded from https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/ and https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/.

### Each file has the columns shown below.
- pmid. The Pubmed ID.
- json. A json summary of the PubmedArticle