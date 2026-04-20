# Cloudflare Intelligence Pipeline

This repository contains the unified **Cloudflare Intelligence Pipeline**, an end-to-end ETL tool for gathering and analyzing developer feedback and competitive visibility data. It merges two core functionalities:
1. **Community Issues Pipeline**: Fetches, categorizes, and analyzes developer friction points from GitHub, StackOverflow, and Reddit.
2. **AI Visibility Pipeline**: Processes SEO and traffic metrics from SEMrush and Adobe Analytics to generate prioritization reports for competitive intelligence.

## Project Structure

```
cloudflare_issue_pipeline/
├── config/
│   └── settings.yaml          # Unified configuration for APIs, LLMs, and routing
├── src/
│   ├── analyzers/             # Relevance classification and sentiment mapping
│   ├── fetchers/              # Community issue API clients (GitHub, Reddit, SO)
│   ├── ingestion/             # Analytics data extractors (SEMrush, Adobe)
│   ├── processing/            # NLP analyzers and prioritization models
│   ├── reporting/             # Markdown executive report generators
│   └── data_writer.py         # CSV/JSON export utilities
├── main.py                    # Primary orchestration script
└── requirements.txt           # Project dependencies
```

## Setup & Installation

1. **Install Dependencies**
   Ensure you have Python 3.8+ installed, then run:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configuration**
   Edit `config/settings.yaml` to include your valid API keys:
   - Community platforms: GitHub Token, Reddit Client ID/Secret, StackOverflow API Key.
   - Analytics: SEMrush API Key, Adobe Analytics Client ID/Secret.
   - LLM: OpenAI API Key.

## Usage

The `main.py` orchestrator supports flags to execute specific parts of the pipeline:

### Run the entire pipeline (Default)
Executes both community issues and visibility pipelines sequentially:
```bash
python main.py --run-all
# or simply:
python main.py
```

### Run Community Issues Pipeline Only
Fetches issues from community platforms, classifies their relevance to Cloudflare products, and generates sentiment scores and simulated prompts:
```bash
python main.py --run-issues
```

### Run AI Visibility Pipeline Only
Ingests keyword and traffic data, analyzes sentiment, extracts citations, and builds a prioritized executive report:
```bash
python main.py --run-visibility
```

## Outputs

- **Issues Output**: Stored in `./data/processed_issues.csv` and `./data/simulated_prompts.json`
- **Visibility Report**: Generated as a Markdown file in `./output/executive_report.md`
