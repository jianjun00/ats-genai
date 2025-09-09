#!/usr/bin/env python3
"""
Add the loadUniverseMembers JavaScript function to the analytics service
"""

def add_universe_members_function():
    file_path = '/home/jianjun/ats-genai-admin/src/services/analytics_service.py'
    
    # Read the current file
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find the location after the loadUniverseAnalytics function to insert the new function
    function_to_add = '''
                async function loadUniverseMembers() {
                    const universeId = document.getElementById('universe-selector').value;
                    const dateFrom = document.getElementById('universe-date-from').value;
                    const dateTo = document.getElementById('universe-date-to').value;
                    
                    if (!universeId) {
                        alert('Please select a universe first.');
                        return;
                    }
                    
                    if (!dateFrom || !dateTo) {
                        alert('Please select both from and to dates.');
                        return;
                    }
                    
                    const membersContent = document.getElementById('universe-members-content');
                    membersContent.innerHTML = '<h4>📊 Universe Members</h4><p>Loading universe members...</p>';
                    
                    try {
                        const response = await fetch(`/api/universe-members/${universeId}?date_from=${dateFrom}&date_to=${dateTo}`);
                        const data = await response.json();
                        
                        if (data.success) {
                            let html = `
                                <h4>📊 Universe Members</h4>
                                <div style="margin-bottom: 15px; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                                    <strong>Universe:</strong> ${data.universe_info.name}<br>
                                    <strong>Description:</strong> ${data.universe_info.description}<br>
                                    <strong>Date Range:</strong> ${dateFrom} to ${dateTo}<br>
                                    <strong>Total Members:</strong> ${data.members.length} symbols
                                </div>
                            `;
                            
                            if (data.members.length > 0) {
                                // Group members by status (active vs historical)
                                const activeMembers = data.members.filter(member => !member.end_at);
                                const historicalMembers = data.members.filter(member => member.end_at);
                                
                                html += `
                                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                                        <div>
                                            <h5 style="color: #388e3c;">✅ Active Members (${activeMembers.length})</h5>
                                            <div style="max-height: 400px; overflow-y: auto; border: 1px solid #ddd; border-radius: 4px;">
                                                <table style="width: 100%; border-collapse: collapse;">
                                                    <thead style="background: #f5f5f5; position: sticky; top: 0;">
                                                        <tr>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Symbol</th>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Start Date</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                `;
                                
                                activeMembers.forEach(member => {
                                    const startDate = new Date(member.start_at).toISOString().split('T')[0];
                                    html += `
                                        <tr>
                                            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; color: #1976d2;">${member.symbol}</td>
                                            <td style="padding: 8px; border: 1px solid #ddd;">${startDate}</td>
                                        </tr>
                                    `;
                                });
                                
                                html += `
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                        
                                        <div>
                                            <h5 style="color: #f57c00;">📋 Historical Members (${historicalMembers.length})</h5>
                                            <div style="max-height: 400px; overflow-y: auto; border: 1px solid #ddd; border-radius: 4px;">
                                                <table style="width: 100%; border-collapse: collapse;">
                                                    <thead style="background: #f5f5f5; position: sticky; top: 0;">
                                                        <tr>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Symbol</th>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Start Date</th>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">End Date</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                `;
                                
                                historicalMembers.forEach(member => {
                                    const startDate = new Date(member.start_at).toISOString().split('T')[0];
                                    const endDate = member.end_at ? new Date(member.end_at).toISOString().split('T')[0] : 'Active';
                                    html += `
                                        <tr>
                                            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; color: #666;">${member.symbol}</td>
                                            <td style="padding: 8px; border: 1px solid #ddd;">${startDate}</td>
                                            <td style="padding: 8px; border: 1px solid #ddd;">${endDate}</td>
                                        </tr>
                                    `;
                                });
                                
                                html += `
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    </div>
                                `;
                            } else {
                                html += `
                                    <div style="text-align: center; padding: 40px; color: #666;">
                                        <p><strong>No members found</strong></p>
                                        <p>The selected universe has no members in the specified date range.</p>
                                    </div>
                                `;
                            }
                            
                            membersContent.innerHTML = html;
                        } else {
                            membersContent.innerHTML = `
                                <h4>📊 Universe Members</h4>
                                <p style="color: red;">Error: ${data.error}</p>
                            `;
                        }
                    } catch (error) {
                        membersContent.innerHTML = `
                            <h4>📊 Universe Members</h4>
                            <p style="color: red;">Error loading universe members: ${error.message}</p>
                        `;
                    }
                }'''
    
    # Find a good insertion point - after loadUniverseAnalytics function
    lines = content.split('\n')
    new_lines = []
    i = 0
    
    while i < len(lines):
        new_lines.append(lines[i])
        
        # Look for the end of loadUniverseAnalytics function
        if 'async function loadUniverseAnalytics()' in lines[i]:
            # Find the matching closing brace
            indent_level = len(lines[i]) - len(lines[i].lstrip())
            i += 1
            
            while i < len(lines):
                new_lines.append(lines[i])
                current_line = lines[i]
                current_indent = len(current_line) - len(current_line.lstrip())
                
                # If we find the closing brace at the same indentation level
                if current_indent == indent_level and current_line.strip() == '}':
                    # Insert the new function after this closing brace
                    new_lines.extend(function_to_add.split('\n'))
                    break
                i += 1
        
        i += 1
    
    # Write the updated content
    with open(file_path, 'w') as f:
        f.write('\n'.join(new_lines))
    
    print("✅ Successfully added loadUniverseMembers function")

if __name__ == "__main__":
    add_universe_members_function()