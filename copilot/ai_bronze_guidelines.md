# Bronze Layer - Auto Loader Pipeline

## Objective

Ingest raw data into a bronze Delta table using Auto Loader.

## Requirements

- Use cloudFiles (Auto Loader)
- Read from Unity Catalog volume path
- Support schema evolution
- Store raw data as-is (minimal transformation)

## Input

- Files will be dropped into a volume location
- Assume format is JSON unless specified otherwise

## Implementation Guidelines

- Use readStream with cloudFiles
- Enable schema inference and evolution
- Add ingestion metadata columns:
  - ingestion_time (current timestamp)
  - source_file (input file name)

## Output

- Write to a Delta table (bronze layer)
- Use checkpointing
- Use append mode

## Example Expectations

- No business logic applied
- No filtering or deduplication
- Preserve raw data fidelity

## Code Structure

- Place logic in a dedicated file: ingestion.py
- Keep function structure clean and minimal

## Anti-patterns to Avoid

- Do not include transformation logic
- Do not hardcode schema unless necessary
- Do not mix bronze and silver logic

## Output Requirements

- Provide full working code
- Include clear function structure
- Use readable configuration (paths, options)