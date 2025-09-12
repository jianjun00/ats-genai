#!/bin/bash
# Script to copy ATS Dashboard JSON to clipboard for easy import

echo "🔄 Copying ATS Dashboard JSON to clipboard..."

# Copy to clipboard (works on Linux with xclip)
if command -v xclip &> /dev/null; then
    cat /home/jianjun/ats-genai-model/config/dashboards/ats-comprehensive-monitoring-dashboard.json | xclip -selection clipboard
    echo "✅ Dashboard JSON copied to clipboard!"
    echo "📋 Now paste it into SignOZ Dashboard Import"
elif command -v pbcopy &> /dev/null; then
    # macOS clipboard
    cat /home/jianjun/ats-genai-model/config/dashboards/ats-comprehensive-monitoring-dashboard.json | pbcopy
    echo "✅ Dashboard JSON copied to clipboard!"
    echo "📋 Now paste it into SignOZ Dashboard Import"
else
    echo "📄 No clipboard tool found. Please manually copy this JSON:"
    echo ""
    cat /home/jianjun/ats-genai-model/config/dashboards/ats-comprehensive-monitoring-dashboard.json
fi

echo ""
echo "🌐 SignOZ Import Steps:"
echo "1. Open: http://localhost:8080"
echo "2. Go to: Dashboards → + New Dashboard → Import"
echo "3. Paste the JSON and click Import"
echo ""
echo "📊 Dashboard Name: 'ATS Comprehensive Services Monitor'"
echo "🎯 Panels: 14 comprehensive monitoring panels"