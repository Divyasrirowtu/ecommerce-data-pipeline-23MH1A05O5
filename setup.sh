cat <<EOF > setup.sh
#!/bin/bash
echo "Setting up environment..."
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
echo "Setup complete"
EOF
#!/bin/bash
echo "Setting up project environment..."

# Create virtual environment (skip if already created)
python -m venv venv

# Activate virtual environment (Git Bash required)
source venv/bin/activate

# Upgrade pip
python -m pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Create folders (in case missed)
mkdir -p data/raw data/staging data/processed
mkdir -p scripts/data_generation scripts/ingestion scripts/transformation scripts/quality_checks
mkdir -p sql/ddl sql/dml sql/queries
mkdir -p dashboards/tableau dashboards/powerbi dashboards/screenshots
mkdir -p logs docs tests .github/workflows

echo "Setup complete!"
