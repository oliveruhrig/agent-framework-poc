# GitHub Copilot Adoption Analyst

This proof of concept uses the [Microsoft Agent Framework](https://github.com/microsoft/agent-framework)
to build a conversational assistant that answers management questions about GitHub Copilot usage
across an engineering organisation. The agent reads historic telemetry you exported from the GitHub
REST / Audit APIs, aggregates the data with `pandas`, and exposes analytics through Azure AI Agents.

## Repository Layout

- `agents/copilot_usage_agent.py` – entry point that spins up the conversational agent
- `services/copilot_usage.py` – analytics layer handling CSV ingestion and metric calculations
- `agents/azure_ai_basic.py` – original quick-start sample (still available for reference)

## Prerequisites

- Python 3.12+ and a virtual environment (the repo already includes `.venv` usage)
- Azure CLI logged in with access to your Azure AI project (`az login`)
- An Azure AI Agent deployment configured for `AZURE_AI_PROJECT_ENDPOINT` and
	`AZURE_AI_MODEL_DEPLOYMENT_NAME`
- Exported CSV data files containing Copilot usage telemetry (see the schemas below)

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
export COPILOT_USAGE_CSV="/path/to/developer_monthly_usage.csv"
export COPILOT_INTERACTIONS_CSV="/path/to/copilot_interactions.csv"
```

## Expected CSV Schemas

**`developer_monthly_usage.csv`**

| column         | description                                         |
| -------------- | --------------------------------------------------- |
| `developer_id` | Unique identifier of the developer                  |
| `division`     | Business division / organisational unit              |
| `month`        | Reporting month in `YYYY-MM` format                  |

**`copilot_interactions.csv`**

| column              | description                                                         |
| ------------------- | ------------------------------------------------------------------- |
| `developer_id`      | Developer identifier                                                |
| `timestamp` or `month` | Either a precise timestamp or a `YYYY-MM` month                  |
| `division` (optional) | Division; joined from the usage CSV if missing                    |
| `model` (optional)  | Copilot model identifier (e.g., `gpt-4o`, `gpt-35-turbo`)           |
| `requests` / `request_count` (optional) | Number of requests represented by each row    |
| `lines_suggested`, `lines_accepted` (optional) | Suggestion acceptance metrics            |

Additional columns are ignored. When request counts are absent the analytics fall back to row
counts. Missing division labels are merged from the monthly usage CSV.

## Running the Agent

```bash
python agents/copilot_usage_agent.py
```

You will enter an interactive prompt. Example questions:

- `What is the Copilot adoption rate in the Platform division for 2025-01 to 2025-06?`
- `Show the organisation-wide adoption trend for the last 6 months.`
- `Which models are used most in the Applications division?`
- `List available divisions`

Type `exit` when you are done.

## Extending the Solution

- Add new CSV sources or database connectors inside `CopilotUsageAnalytics`
- Promote the analytics layer into a reusable MCP server if other agents or Copilot Chat
	experiences should call the same tools
- Introduce persistent conversation storage (Cosmos DB / Redis) to share context across sessions
- Connect Azure AI Search or a semantic cache for narrative summaries and glossary lookups

## Troubleshooting

- **Missing data** – ensure the CSV paths are correct and follow the expected schema
- **Authentication failures** – rerun `az login` and verify the Azure AI project endpoint & deployment
- **Dependency errors** – reinstall requirements within your virtual environment
