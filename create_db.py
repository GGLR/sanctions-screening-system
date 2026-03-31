import sqlite3
from pathlib import Path
import xml.etree.ElementTree as ET
import hashlib
from datetime import datetime

# Create database
db_path = Path('data/sanctions.db')
conn = sqlite3.connect(str(db_path))
cursor = conn.cursor()

# Create table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sanctions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unique_hash TEXT UNIQUE NOT NULL,
        full_name TEXT NOT NULL,
        date_of_birth TEXT,
        nationality TEXT,
        id_number TEXT,
        id_type TEXT,
        source TEXT NOT NULL,
        listing_date TEXT,
        comments TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        is_active INTEGER DEFAULT 1
    )
''')
conn.commit()

# Parse MOHA
print('Loading MOHA...')
# Fix malformed XML
with open('moha_sanctions_list.xml', 'r', encoding='utf-8') as f:
    content = f.read()
content = content.replace('</xmlResponse>', '')

root = ET.fromstring(content)
moha_count = 0

for entry in root.findall('.//entry'):
    full_name = ''
    dob = ''
    nationality = ''
    id_number = ''
    
    for field in entry.findall('field'):
        name_attr = field.get('name', '')
        value = field.text or ''
        
        if 'Name' in name_attr and full_name == '':
            full_name = value
        elif 'Date of Birth' in name_attr and dob == '':
            dob = value
        elif 'Nationality' in name_attr and nationality == '':
            nationality = value
        elif ('Passport' in name_attr or 'Identification' in name_attr) and id_number == '':
            id_number = value
    
    if full_name:
        unique_hash = hashlib.md5(f'{full_name}|{dob}|{id_number}'.encode()).hexdigest()
        now = datetime.now().isoformat()
        try:
            cursor.execute('''
                INSERT INTO sanctions (unique_hash, full_name, date_of_birth, nationality, id_number, source, created_at, updated_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            ''', (unique_hash, full_name, dob, nationality, id_number, 'MOHA_MALAYSIA', now, now))
            moha_count += 1
        except:
            pass

conn.commit()
print(f'MOHA: {moha_count} records')

# Parse UN
print('Loading UN...')
tree = ET.parse('un_sanctions_list.xml')
root = tree.getroot()
un_count = 0

for entry in root.findall('.//INDIVIDUAL'):
    first = entry.find('.//FIRST_NAME')
    last = entry.find('.//LAST_NAME')
    
    full_name = ''
    if first is not None and first.text:
        full_name += first.text + ' '
    if last is not None and last.text:
        full_name += last.text
    full_name = full_name.strip()
    
    if not full_name:
        continue
    
    dob = ''
    dob_elem = entry.find('.//DATE_OF_BIRTH')
    if dob_elem is not None and dob_elem.text:
        dob = dob_elem.text
    
    nationality = ''
    nat_elem = entry.find('.//NATIONALITY')
    if nat_elem is not None and nat_elem.text:
        nationality = nat_elem.text
    
    id_number = ''
    id_elem = entry.find('.//NUMBER')
    if id_elem is not None and id_elem.text:
        id_number = id_elem.text
    
    unique_hash = hashlib.md5(f'{full_name}|{dob}|{id_number}'.encode()).hexdigest()
    now = datetime.now().isoformat()
    try:
        cursor.execute('''
            INSERT INTO sanctions (unique_hash, full_name, date_of_birth, nationality, id_number, source, created_at, updated_at, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
        ''', (unique_hash, full_name, dob, nationality, id_number, 'UN_LIST', now, now))
        un_count += 1
    except:
        pass

conn.commit()
print(f'UN: {un_count} records')

# Parse PEP list
print('Loading PEP list...')
pep_tree = ET.parse('pep_list.xml')
pep_root = pep_tree.getroot()
pep_count = 0

for person in pep_root.findall('.//Person'):
    name_el = person.find('Name')
    full_name = name_el.text.strip() if name_el is not None and name_el.text else ''

    if not full_name:
        continue

    dob = ''
    dob_el = person.find('DateOfBirth')
    if dob_el is not None and dob_el.text:
        dob = dob_el.text.strip()

    nationality = ''
    nat_el = person.find('Nationality')
    if nat_el is not None and nat_el.text:
        nat_text = nat_el.text.strip().upper()
        if nat_text != 'UNKNOWN':
            nationality = nat_text

    # Build comments from Position, Organization, SourceURL
    comments_parts = []
    pos_el = person.find('Position')
    if pos_el is not None and pos_el.text:
        pos_text = pos_el.text.strip()
        if pos_text.upper() != 'UNKNOWN':
            comments_parts.append(f'Position: {pos_text}')
    org_el = person.find('Organization')
    if org_el is not None and org_el.text:
        org_text = org_el.text.strip()
        if org_text.upper() not in ('UNKNOWN', 'ON BEHALF OF'):
            comments_parts.append(f'Organization: {org_text}')
    src_el = person.find('SourceURL')
    if src_el is not None and src_el.text:
        comments_parts.append(f'Source: {src_el.text.strip()}')
    comments = ' | '.join(comments_parts) if comments_parts else None

    unique_hash = hashlib.md5(f'{full_name}|{dob}|'.encode()).hexdigest()
    now = datetime.now().isoformat()
    try:
        cursor.execute('''
            INSERT INTO sanctions (unique_hash, full_name, date_of_birth, nationality, id_number, source, listing_date, comments, created_at, updated_at, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        ''', (unique_hash, full_name, dob, nationality, None, 'PEP_LIST', None, comments, now, now))
        pep_count += 1
    except:
        pass

conn.commit()
print(f'PEP: {pep_count} records')

# Get final counts
cursor.execute('SELECT source, COUNT(*) FROM sanctions WHERE is_active = 1 GROUP BY source')
counts = cursor.fetchall()

print(f'\nDatabase created!')
for source, count in counts:
    print(f'{source}: {count}')

cursor.execute('SELECT COUNT(*) FROM sanctions WHERE is_active = 1')
print(f'Total: {cursor.fetchone()[0]}')

conn.close()
