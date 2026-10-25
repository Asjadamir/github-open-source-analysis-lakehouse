```mermaid
erDiagram

    GOLD_DIM_REPOSITORY ||--o{
        GOLD_FACT_REPOSITORY_SNAPSHOT :
        repo_id

    GOLD_DIM_DATE ||--o{
        GOLD_FACT_REPOSITORY_SNAPSHOT :
        date_key

    GOLD_DIM_REPOSITORY ||--o{
        GOLD_FACT_REPOSITORY_ACTIVITY :
        repo_id

    GOLD_DIM_DATE ||--o{
        GOLD_FACT_REPOSITORY_ACTIVITY :
        date_key

    GOLD_DIM_DATE ||--o{
        GOLD_FACT_PIPELINE_EXECUTION:
        date_key



Explain:

```text
gold_dim_repository
PK: repo_id

gold_dim_date
PK: date_key

gold_fact_repository_snapshot
PK: repo_id + date_key

gold_fact_repository_activity
PK: repo_id + date_key

gold_fact_pipeline_execution
PK: run_id
