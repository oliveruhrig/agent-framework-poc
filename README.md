# GitHub Copilot Adoption Analyst

This proof of concept uses the [Microsoft Agent Framework](https://github.com/microsoft/agent-framework)
to build a conversational assistant that answers management questions about GitHub Copilot usage
across an engineering organisation. The agent reads historic telemetry you exported from the GitHub
REST / Audit APIs, aggregates the data with `pandas`, and exposes analytics through Azure AI Agents.

## Repository Layout

- `agents/orchestrator.py` – Microsoft Agent Framework orchestrator that calls the MCP tools
- `mcp/copilot_usage_server.py` – MCP server exposing segment-level adoption analytics
- `services/segment_adoption.py` & `services/segment_adoption_loader.py` – analytics layer and loader for the FTE vs contractor dataset
- `services/metrics_registry.py` & `config/metrics.yaml` – governance catalogue for key metrics
- `agents/azure_ai_basic.py` – original quick-start sample for reference

## Prerequisites

- Python 3.12+ and a virtual environment (the repo already includes `.venv` usage)
- Azure CLI logged in with access to your Azure AI project (`az login`)
- An Azure AI Agent deployment configured for `AZURE_AI_PROJECT_ENDPOINT` and
	`AZURE_AI_MODEL_DEPLOYMENT_NAME`
- Exported CSV data file containing Copilot segment-level adoption telemetry (see the schema below)

Install dependencies:

```bash
pip install -r requirements.txt
```

## Required Environment Variables

Set the following values in your shell or `.env` file:

```bash
export AZURE_AI_PROJECT_ENDPOINT="https://<your-project>.openai.azure.com/"
export AZURE_AI_MODEL_DEPLOYMENT_NAME="<deployment-name>"

# Optional overrides – defaults use ./data/*.csv
export COPILOT_SEGMENT_ADOPTION_CSV="/path/to/segment_adoption.csv"
```

## Expected CSV Schemas

**`segment_adoption.csv`** (aggregated FTE vs contractor telemetry)

| column                    | description                                                      |
| ------------------------- | ---------------------------------------------------------------- |
| `Month`                   | Reporting month in `YYYY-MM` format                              |
| `Segment`                 | Business segment / organisational unit                           |
| `Active_users_FTE`        | Count of active full-time employee users                         |
| `Active_users_nonFTE`     | Count of active contractor users                                 |
| `total_seats_FTE`         | Number of FTE seats available                                    |
| `total_seats_nonFTE`      | Number of contractor seats available                             |
| `billing_adoption_FTE`    | Percentage of FTE seats covered by the billing programme         |
| `billing_adoption_nonFTE` | Percentage of contractor seats covered by the billing programme  |

Whitespace, commas, and percentage signs are normalised automatically. Use `COPILOT_SEGMENT_ADOPTION_CSV`
to point at the exported file under `data/copilot/segment_adoption.csv`.

## Running the Solution

Start the MCP server (requires `az login` and access to the CSV files):

```bash
python -m mcp.copilot_usage_server
```

In a separate terminal, launch the orchestrator agent (the `--pre` flag is required when installing
`agent-framework`):

```bash
python agents/orchestrator.py
```

You will enter an interactive prompt. Example questions:

- `Summarise FTE vs contractor adoption for Asia in 2025.`
- `Compare FTE versus contractor adoption in Asia for 2025.`
- `Describe the metrics captured by fte_utilisation and contractor_billing_adoption.`

Type `exit` when you are done. Responses are grounded in MCP tool outputs; if the governance guard
detects an unsafe request it will refuse the prompt before the agent calls the tools.

## Extending the Solution

- Extend segment adoption ingestion or derived metrics inside `services/segment_adoption.py`
- Scale the MCP server with authentication, caching, and additional tools (e.g., chart specs)
- Introduce persistent conversation storage (Cosmos DB / Redis) to share context across sessions
- Connect Azure AI Search or a semantic cache for narrative summaries and glossary lookups

## Troubleshooting

- **Missing data** – ensure the CSV paths are correct and follow the expected schema
- **Authentication failures** – rerun `az login` and verify the Azure AI project endpoint & deployment
- **Dependency errors** – reinstall requirements within your virtual environment
