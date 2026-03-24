# AI Engineering Guidelines (Databricks / PySpark)

You are a senior data engineer designing production-grade data pipelines.

## Core Principles

- Write clean, modular, and maintainable Python code
- Keep functions small, single-responsibility
- Avoid monolithic pipeline code

## Separation of Concerns

- Separate transformation logic from orchestration
- Do NOT embed business logic inside DLT decorators or streaming reads
- Transformation functions must be reusable and testable

## Spark Best Practices

- Avoid using global Spark session inside transformation functions
- Write transformations using DataFrame APIs (avoid SQL strings where possible)
- Ensure transformations are deterministic and idempotent

## Testing

- All transformation logic must be unit testable using pytest
- Use small, deterministic datasets for testing
- Prefer DataFrame equality checks (e.g., chispa)

## Data Engineering Standards

- Design for incremental processing
- Handle nulls and edge cases explicitly
- Ensure schema consistency
- Avoid unnecessary shuffles or wide transformations

## Code Quality

- Use meaningful variable and function names
- Add concise comments where necessary
- Ensure code is compatible with black and ruff formatting

## Output Expectations

- Organize code into multiple files (not a single script)
- Keep pipeline orchestration thin
- Prioritize readability over cleverness