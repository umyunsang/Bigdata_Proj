#!/bin/bash
#
# Setup Daily Collection Automation
# Configures cron job to run daily random collection at 8:00 AM
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
COLLECTION_SCRIPT="$SCRIPT_DIR/daily_random_collection.py"
LOG_DIR="$PROJECT_DIR/logs/collection"

# Create log directory
mkdir -p "$LOG_DIR"

# Cron job entry
CRON_COMMAND="0 8 * * * cd $PROJECT_DIR && $PYTHON_BIN $COLLECTION_SCRIPT --quota 9500 >> $LOG_DIR/daily_collection_\$(date +\%Y\%m\%d).log 2>&1"

echo "="
echo "Setting up daily collection automation"
echo "="

echo "Project directory: $PROJECT_DIR"
echo "Python binary: $PYTHON_BIN"
echo "Collection script: $COLLECTION_SCRIPT"
echo "Log directory: $LOG_DIR"
echo ""
echo "Cron job (runs at 8:00 AM daily):"
echo "$CRON_COMMAND"
echo ""

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "daily_random_collection.py"; then
    echo "⚠️  Cron job already exists!"
    echo "Current cron jobs:"
    crontab -l | grep "daily_random_collection"
else
    echo "Adding cron job..."
    (crontab -l 2>/dev/null; echo "$CRON_COMMAND") | crontab -
    echo "✅ Cron job added successfully!"
fi

echo ""
echo "To verify:"
echo "  crontab -l | grep daily_random"
echo ""
echo "To remove:"
echo "  crontab -e  # then delete the line manually"
echo ""
echo "Manual execution:"
echo "  cd $PROJECT_DIR"
echo "  $PYTHON_BIN $COLLECTION_SCRIPT --quota 9500"
