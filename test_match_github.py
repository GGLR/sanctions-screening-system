import sys
sys.path.insert(0, '.')
import sqlite3

output = []

# Direct database check
conn = sqlite3.connect('sanctions.db')
cursor = conn.cursor()
cursor.execute("SELECT full_name FROM sanctions WHERE full_name LIKE '%MOHAMAD IQBAL%'")
rows = cursor.fetchall()
conn.close()

output.append(f"GitHub database check:")
for row in rows:
    output.append(f"  Found: {row[0]}")

# Test matching
from matching_engine import FuzzyMatchingEngine
engine = FuzzyMatchingEngine()

query_name = 'mohamad iqbal abdurrahman'
target_name = 'MOHAMAD IQBAL ABDURRAHMAN'

normalized_query = engine._normalize_name(query_name)
normalized_target = engine._normalize_name(target_name)

output.append(f"Query (normalized): {normalized_query}")
output.append(f"Target (normalized): {normalized_target}")

if normalized_query == normalized_target:
    output.append("Exact match after normalization!")
    
score, match_type = engine._calculate_name_score(query_name, target_name)
output.append(f"Score: {score}, Type: {match_type}")

# Write to file
with open('test_output_github.txt', 'w') as f:
    f.write('\n'.join(output))