# Testing Guide

This directory contains the test suite for the GitHub Copilot analytics system.

## Test Structure

- `test_mcp_server.py` - Functional tests for MCP server endpoints
- `test_premium_requests.py` - Unit tests for premium requests analytics

## Running Tests

### Install test dependencies

```bash
pip install pytest httpx
```

### Run all tests

```bash
pytest tests/
```

### Run specific test file

```bash
pytest tests/test_mcp_server.py
pytest tests/test_premium_requests.py
```

### Run with verbose output

```bash
pytest tests/ -v
```

### Run with coverage

```bash
pip install pytest-cov
pytest tests/ --cov=services --cov=mcp --cov-report=html
```

## Test Categories

### Functional Tests (test_mcp_server.py)

These tests require:
- MCP server running on `http://127.0.0.1:8000`
- CSV data files present in `data/copilot/`

Start the server before running:
```bash
cd mcp
python copilot_usage_server.py
```

Tests cover:
- Health endpoint verification
- All 10 MCP tools (5 segment adoption + 5 premium requests)
- Tool listing endpoint
- Error handling for invalid requests
- Parametrized tests for different filter combinations

### Unit Tests (test_premium_requests.py)

These tests are standalone and don't require the server:
- Use temporary CSV files with sample data
- Test data loading and validation
- Test all analytics methods
- Test filtering logic (segment, user_type, date ranges)
- Test error handling for invalid inputs
- Test data cleaning and normalization

## Test Data

Unit tests use fixtures with sample data. For functional tests, ensure your CSV files are present:
- `data/copilot/segment_adoption.csv`
- `data/copilot/premium_requests_db.csv`

## Continuous Integration

To skip server-dependent tests (useful for CI without server running):
```bash
pytest tests/ -m "not server"
```

Mark server tests with `@pytest.mark.server` decorator if needed.

## Common Issues

1. **Connection refused errors**: Make sure MCP server is running
2. **Missing CSV files**: Check `.env` file has correct paths
3. **Import errors**: Ensure project root is in PYTHONPATH

## Adding New Tests

When adding new MCP tools:
1. Add functional test in `test_mcp_server.py`
2. Add unit test for analytics logic in appropriate test file
3. Update this README with new test coverage
