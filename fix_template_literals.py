#!/usr/bin/env python3
"""
Fix escaped template literals in analytics service
"""

def fix_template_literals():
    file_path = '/home/jianjun/ats-genai-model/src/services/analytics_service.py'

    with open(file_path, 'r') as f:
        content = f.read()

    # Replace \${ with ${
    fixed_content = content.replace('\\${', '${')

    with open(file_path, 'w') as f:
        f.write(fixed_content)

    print("Fixed escaped template literals")

if __name__ == "__main__":
    fix_template_literals()