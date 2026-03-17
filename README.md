# AZ Data Agent

AI-powered data analysis system for AstraZeneca pharmaceutical data.

## Features

- **Intelligent Monitoring**: Scheduled SQL metric execution with threshold-based alerting
- **AI Analysis**: LangChain-powered agent for natural language data queries
- **Auto-Alert Analysis**: Automatic deep-dive analysis when thresholds are breached
- **Web Interface**: Streamlit-based chat interface

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your credentials

# Initialize database
python scripts/init_db.py

# Start services
./scripts/start_all.sh
```

## Architecture

See [Design Document](docs/superpowers/specs/2026-03-16-az-data-agent-design.md)
 
## License

MIT
