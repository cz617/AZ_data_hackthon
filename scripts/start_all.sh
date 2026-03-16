#!/bin/bash
# Start all services

# Create data directory
mkdir -p data

# Initialize database if needed
python scripts/init_db.py

# Load metrics from config (if Python available)
python -c "from src.monitor.metrics_loader import reload_metrics_from_config; reload_metrics_from_config()" 2>/dev/null || echo "Skipping metrics load"

# Start Streamlit
echo "Starting AZ Data Agent on http://localhost:8501"
streamlit run src/web/app.py --server.port 8501