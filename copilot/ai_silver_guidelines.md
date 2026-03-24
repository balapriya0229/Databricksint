# Silver Layer - Transformation Pipeline

## Objective

Transform bronze data into clean, deduplicated, business-ready silver data.

## Key Principles

- ALL transformations must be implemented as pure functions
- Functions must accept and return DataFrames
- No direct dependency on streaming or DLT inside transformation logic

## Required Transformations

- Remove duplicates based on primary key (e.g., customer_id)
  - Keep latest record using updated_at
- Filter out invalid records:
  - null primary keys
  - null critical fields (e.g., email)
- Standardize fields:
  - lowercase email
  - trim strings where needed

## Deduplication Pattern

- Use window functions
- Partition by business key
- Order by updated_at DESC
- Keep latest record

## Code Structure

- transforms.py → all transformation logic
- silver.py → pipeline orchestration

## Example Pattern

- silver.py:
  - read bronze table
  - call transformation functions
  - write to silver table

## Testing Requirements

- Provide pytest unit tests for transformation functions
- Do NOT include streaming or Auto Loader in tests
- Use small sample datasets
- Validate:
  - deduplication
  - filtering
  - transformations

## Data Quality

- Ensure output schema consistency
- Handle nulls explicitly
- Avoid silent data drops

## Anti-patterns

- Do not embed logic inside DLT decorators
- Do not write transformations inline in pipeline
- Do not use collect() in transformations

## Output Requirements

- Provide:
  - transforms.py
  - silver.py
  - test_transforms.py
- Code must be modular, readable, and production-ready