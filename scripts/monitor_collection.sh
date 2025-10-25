#!/bin/bash
#
# Monitor Daily Collection Progress
#

TODAY=$(date +%Y%m%d)
LOGFILE="logs/collection/day1_${TODAY}.log"

if [ ! -f "$LOGFILE" ]; then
    echo "❌ Log file not found: $LOGFILE"
    exit 1
fi

echo "========================================"
echo "📊 Collection Progress Monitor"
echo "========================================"
echo "Log file: $LOGFILE"
echo ""

# Check if process is running
if pgrep -f "daily_random_collection.py" > /dev/null; then
    echo "✅ Collection process is running"
else
    echo "⚠️  Collection process not found"
fi

echo ""
echo "Latest progress:"
tail -10 "$LOGFILE"

echo ""
echo "----------------------------------------"
echo "Statistics:"
echo "----------------------------------------"

# Extract stats
LAST_CALL=$(grep -oP 'Call \K\d+/\d+' "$LOGFILE" | tail -1)
LAST_UNIQUE=$(grep -oP 'Unique videos: \K\d+' "$LOGFILE" | tail -1)
LAST_QUOTA=$(grep -oP 'quota used: \K\d+' "$LOGFILE" | tail -1)

echo "Last API call: $LAST_CALL"
echo "Unique videos collected: $LAST_UNIQUE"
echo "QPD used: $LAST_QUOTA / 9500"

# Calculate progress
if [ -n "$LAST_QUOTA" ]; then
    PROGRESS=$(echo "scale=1; $LAST_QUOTA * 100 / 9500" | bc)
    echo "Progress: ${PROGRESS}%"
fi

echo ""
echo "To watch live:"
echo "  tail -f $LOGFILE"
