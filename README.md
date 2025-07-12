## Transient Table Analysis using LLM
A lightweight pipeline that converts semi‑structured domain knowledge (e.g., JSON dumps or Wikidata snapshots) into fully‑queryable SQLite databases – and ships with curated natural‑language question/answer pairs so you can immediately start benchmarking your SQL‑generating LLMs.
### Key Features
1. Domain‑aware ETL – Transform raw JSON/CSV into first‑normal‑form tables with appropriate primary/foreign keys.
2. Automatic schema generation – Uses the metadata in each JSON file to produce `CREATE TABLE` statements.
3. LM‑ready datasets – 1000+ human‑written questions with ground‑truth SQL answers for the various domains (see `question‑answer.csv`).
4. Gemini integration – The `json‑processor` package can call Google Gemini (`google‑generativeai`) to help with column naming and type inference.

### Repository Layout
```
SQL-Extraction/
├── domain_dbs/               
├── json-processor/       
├── question-answer.csv       
├── transient.ipynb       
└── .DS_Store
```
