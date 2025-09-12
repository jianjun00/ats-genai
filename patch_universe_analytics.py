#!/usr/bin/env python3
"""
Quick patch to update the Universe Analytics function in the main analytics service
"""

import re

def patch_universe_analytics():
    file_path = '/home/jianjun/ats-genai-admin/src/services/analytics_service.py'

    # Read the current file
    with open(file_path, 'r') as f:
        content = f.read()

    # Find the loadUniverseAnalytics function and replace it
    pattern = r'async function loadUniverseAnalytics\(\) \{.*?^\s+\}'

    replacement = '''async function loadUniverseAnalytics() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>🌐 Universe Analytics</h3><p>Loading universe selection menu...</p>';

                    try {
                        // Load available universes
                        const universesResponse = await fetch('/api/universes');
                        const universesData = await universesResponse.json();

                        if (universesData.success) {
                            let html = `
                                <h3>🌐 Universe Analytics</h3>
                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4>🔍 Universe Selection</h4>
                                    <div style="display: grid; grid-template-columns: 2fr 1fr 1fr auto; gap: 15px; align-items: end; margin-bottom: 15px;">
                                        <div>
                                            <label for="universe-selector" style="display: block; margin-bottom: 5px; font-weight: bold;">Select Universe:</label>
                                            <select id="universe-selector" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                                <option value="">-- Select a universe --</option>
                            `;

                            universesData.universes.forEach(universe => {
                                html += `<option value="${universe.id}">${universe.name} - ${universe.description}</option>`;
                            });

                            html += `
                                            </select>
                                        </div>
                                        <div>
                                            <label for="universe-date-from" style="display: block; margin-bottom: 5px; font-weight: bold;">From Date:</label>
                                            <input type="date" id="universe-date-from" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                        </div>
                                        <div>
                                            <label for="universe-date-to" style="display: block; margin-bottom: 5px; font-weight: bold;">To Date:</label>
                                            <input type="date" id="universe-date-to" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                        </div>
                                        <div>
                                            <button onclick="loadUniverseMembers()" style="padding: 8px 16px; background: #4285f4; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                                Load Members
                                            </button>
                                        </div>
                                    </div>
                                    <p style="color: #666; font-size: 0.9em; margin: 0;">
                                        <strong>Available Universes:</strong> ${universesData.universes.length} total
                                    </p>
                                </div>

                                <div id="universe-members-content" style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📊 Universe Members</h4>
                                    <p style="color: #666;">Select a universe and date range above to view members.</p>
                                </div>
                            `;

                            document.getElementById('analysis-content').innerHTML = html;

                            // Set default date range (last 30 days)
                            const today = new Date();
                            const thirtyDaysAgo = new Date(today);
                            thirtyDaysAgo.setDate(today.getDate() - 30);

                            document.getElementById('universe-date-from').value = thirtyDaysAgo.toISOString().split('T')[0];
                            document.getElementById('universe-date-to').value = today.toISOString().split('T')[0];
                        }
                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>🌐 Universe Analytics</h3><p style="color: red;">Error loading universe analytics: ' + error.message + '</p>';
                    }
                }'''

    # Replace the function using a more flexible approach
    lines = content.split('\n')
    new_lines = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check if this line starts the loadUniverseAnalytics function
        if 'async function loadUniverseAnalytics()' in line:
            # Add the new function instead
            new_lines.extend(replacement.split('\n'))

            # Skip all lines until we find the closing brace at the same indentation level
            indent_level = len(line) - len(line.lstrip())
            i += 1

            while i < len(lines):
                current_line = lines[i]
                current_indent = len(current_line) - len(current_line.lstrip())

                # If we find a line with the same indentation level that ends with }, we found the end
                if current_indent == indent_level and current_line.strip() == '}':
                    i += 1  # Skip the closing brace
                    break
                i += 1
        else:
            new_lines.append(line)
            i += 1

    # Write the updated content
    with open(file_path, 'w') as f:
        f.write('\n'.join(new_lines))

    print("✅ Successfully patched Universe Analytics function")

if __name__ == "__main__":
    patch_universe_analytics()