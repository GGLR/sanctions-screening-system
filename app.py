"""
Standalone Streamlit App for Sanctions List Screening System
Optimized for Streamlit Cloud deployment - no API backend required
"""

import streamlit as st
import sqlite3
import os
import re
import json
import time
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any
from rapidfuzz import fuzz
from rapidfuzz.fuzz import partial_ratio, token_sort_ratio, token_set_ratio
import pandas as pd
from xml.etree import ElementTree as ET

# ============================================================
# CONFIGURATION
# ============================================================

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "sanctions.db")
MOHA_URL = "https://www.moha.gov.my/utama/images/Terkini/SENARAI_KDN_2026_BI_1.xml"
UN_URL = "https://desaprod.un.org/2.0/resources/consolidated"

# Default XML files to auto-load
MOHA_XML_FILE = os.path.join(BASE_DIR, "moha_sanctions_list.xml")
UN_XML_FILE = os.path.join(BASE_DIR, "un_sanctions_list.xml")

# Matching thresholds
MATCH_THRESHOLD = 70  # Minimum score to show as match
HIGH_RISK_THRESHOLD = 85
MEDIUM_RISK_THRESHOLD = 70

# ============================================================
# DATABASE FUNCTIONS
# ============================================================

def get_db_connection():
    """Get database connection"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Initialize database schema"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sanctions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            normalized_name TEXT,
            date_of_birth TEXT,
            nationality TEXT,
            id_number TEXT,
            source TEXT,
            list_type TEXT,
            listing_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(full_name, id_number, source)
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_normalized_name ON sanctions(normalized_name)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_id_number ON sanctions(id_number)
    """)
    
    conn.commit()
    conn.close()

def get_all_sanctions() -> List[Dict]:
    """Get all sanctions from database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sanctions ORDER BY full_name")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_sanction_count() -> Dict:
    """Get count of sanctions by source"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT source, COUNT(*) as count FROM sanctions GROUP BY source")
    sources = cursor.fetchall()
    
    cursor.execute("SELECT COUNT(*) as total FROM sanctions")
    total = cursor.fetchone()["total"]
    
    conn.close()
    return {
        "total": total,
        "sources": {row["source"]: row["count"] for row in sources}
    }

def add_sanction(full_name: str, date_of_birth: str = None, nationality: str = None, 
                id_number: str = None, source: str = "MANUAL", list_type: str = "OTHER"):
    """Add a single sanction record"""
    normalized = normalize_name(full_name)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO sanctions 
            (full_name, normalized_name, date_of_birth, nationality, id_number, source, list_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (full_name.upper(), normalized, date_of_birth, nationality, id_number, source, list_type))
        conn.commit()
        result = cursor.rowcount > 0
    except Exception as e:
        st.error(f"Error adding sanction: {e}")
        result = False
    finally:
        conn.close()
    
    return result

def add_bulk_sanctions(records: List[Dict]) -> int:
    """Add multiple sanction records"""
    conn = get_db_connection()
    cursor = conn.cursor()
    added = 0
    
    for record in records:
        normalized = normalize_name(record.get("full_name", ""))
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO sanctions 
                (full_name, normalized_name, date_of_birth, nationality, id_number, source, list_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                record.get("full_name", "").upper(),
                normalized,
                record.get("date_of_birth"),
                record.get("nationality"),
                record.get("id_number"),
                record.get("source", "MANUAL"),
                record.get("list_type", "OTHER")
            ))
            if cursor.rowcount > 0:
                added += 1
        except:
            pass
    
    conn.commit()
    conn.close()
    return added

def delete_sanction(sanction_id: int) -> bool:
    """Delete a sanction record"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sanctions WHERE id = ?", (sanction_id,))
    conn.commit()
    result = cursor.rowcount > 0
    conn.close()
    return result

def clear_all_sanctions() -> bool:
    """Clear all sanctions from database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sanctions")
    conn.commit()
    conn.close()
    return True

def auto_load_xml_files() -> Dict[str, int]:
    """Auto-load XML files if they exist in the app directory"""
    loaded = {"moha": 0, "un": 0}
    
    # Check if database is empty
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sanctions")
    count = cursor.fetchone()[0]
    conn.close()
    
    if count > 0:
        # Database already has data, skip auto-load
        return loaded
    
    # Try to load MOHA XML
    if os.path.exists(MOHA_XML_FILE):
        try:
            with open(MOHA_XML_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
            records = parse_moha_xml(content)
            if records:
                loaded["moha"] = add_bulk_sanctions(records)
        except Exception as e:
            st.warning(f"Could not load MOHA XML: {e}")
    
    # Try to load UN XML
    if os.path.exists(UN_XML_FILE):
        try:
            with open(UN_XML_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
            records = parse_un_xml(content)
            if records:
                loaded["un"] = add_bulk_sanctions(records)
        except Exception as e:
            st.warning(f"Could not load UN XML: {e}")
    
    return loaded

def parse_un_xml(xml_content: str) -> List[Dict]:
    """Parse UN sanctions list XML"""
    records = []
    
    try:
        root = ET.fromstring(xml_content)
        
        # Parse UN XML format: <INDIVIDUALS><INDIVIDUAL>...</INDIVIDUAL></INDIVIDUALS>
        individuals = root.findall(".//INDIVIDUAL")
        
        for person in individuals:
            # Get first and second name
            first_name = person.find("FIRST_NAME")
            second_name = person.find("SECOND_NAME")
            
            name_parts = []
            if first_name is not None and first_name.text:
                name_parts.append(first_name.text.strip())
            if second_name is not None and second_name.text:
                name_parts.append(second_name.text.strip())
            
            if not name_parts:
                continue
                
            full_name = " ".join(name_parts)
            
            # Skip if name is too short
            if len(full_name) < 3:
                continue
            
            # Get DOB - look for YEAR in INDIVIDUAL_DATE_OF_BIRTH
            dob = None
            dob_elem = person.find("INDIVIDUAL_DATE_OF_BIRTH")
            if dob_elem is not None:
                year_elem = dob_elem.find("YEAR")
                if year_elem is not None and year_elem.text:
                    dob = year_elem.text
            
            # Get nationality - look for VALUE in NATIONALITY
            nationality = None
            nat_elem = person.find("NATIONALITY")
            if nat_elem is not None:
                val_elem = nat_elem.find("VALUE")
                if val_elem is not None and val_elem.text:
                    nationality = val_elem.text.strip()
            
            # Get ID/Document number
            id_number = None
            doc_elem = person.find("INDIVIDUAL_DOCUMENT")
            if doc_elem is not None:
                doc_num = doc_elem.find("NUMBER")
                if doc_num is not None and doc_num.text:
                    id_number = doc_num.text.strip()
            
            # Get reference number as alternative ID
            if not id_number:
                ref_elem = person.find("REFERENCE_NUMBER")
                if ref_elem is not None and ref_elem.text:
                    id_number = ref_elem.text.strip()
            
            records.append({
                "full_name": full_name,
                "date_of_birth": dob,
                "nationality": nationality,
                "id_number": id_number,
                "source": "UN",
                "list_type": "UN"
            })
    
    except Exception as e:
        pass  # Silently fail for UN XML parsing issues
    
    return records

# ============================================================
# NAME NORMALIZATION & MATCHING
# ============================================================

def normalize_name(name: str) -> str:
    """Normalize name for matching"""
    if not name:
        return ""
    
    # Convert to uppercase and remove extra spaces
    name = name.upper().strip()
    
    # Remove common prefixes/titles
    prefixes = ['DATO', 'DATIN', 'TAN SRI', 'TUN', 'EN', 'PN', 'SITI', 'SIR', 'MR', 'MRS', 'MS', 'DR', 'PROF']
    words = name.split()
    if words and words[0] in prefixes:
        words = words[1:]
    
    # Remove special characters but keep spaces
    name = re.sub(r'[^A-Z\s]', '', name)
    
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    return name

def clean_dob(dob: str) -> str:
    """Clean and standardize date of birth"""
    if not dob:
        return None
    
    dob = dob.strip()
    
    # Try various date formats
    formats = ['%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d %B %Y', '%B %d, %Y']
    
    for fmt in formats:
        try:
            dt = datetime.strptime(dob, fmt)
            return dt.strftime('%d-%m-%Y')
        except:
            continue
    
    return dob

def calculate_name_score(query_name: str, sanction_name: str) -> float:
    """Calculate name similarity score with strict fuzzy matching"""
    if not query_name or not sanction_name:
        return 0.0
    
    q_norm = normalize_name(query_name)
    s_norm = normalize_name(sanction_name)
    
    if not q_norm or not s_norm:
        return 0.0
    
    # Exact match
    if q_norm == s_norm:
        return 100.0
    
    # Check if query is contained in sanction (full word match)
    q_words = set(q_norm.split())
    s_words = set(s_norm.split())
    
    # All query words must be in sanction words
    if q_words.issubset(s_words):
        return 100.0
    
    # Check for exact word matches first
    exact_word_matches = sum(1 for w in q_words if w in s_words)
    if exact_word_matches > 0 and exact_word_matches == len(q_words):
        return 100.0
    
    # Apply 20% penalty for fuzzy matches (non-exact)
    base_score = max(token_sort_ratio(q_norm, s_norm), token_set_ratio(q_norm, s_norm))
    
    # Strict: require at least 70% for fuzzy to count
    if base_score >= 70:
        return base_score * 0.8  # 20% penalty
    
    return 0.0

def calculate_id_match(query_id: str, sanction_id: str) -> bool:
    """Check if ID numbers match exactly"""
    if not query_id or not sanction_id:
        return False
    
    # Normalize: remove spaces and dashes
    q_id = re.sub(r'[\s\-]', '', query_id.upper())
    s_id = re.sub(r'[\s\-]', '', sanction_id.upper())
    
    return q_id == s_id

def calculate_dob_match(query_dob: str, sanction_dob: str) -> bool:
    """Check if dates of birth match"""
    if not query_dob or not sanction_dob:
        return False
    
    q_dob = clean_dob(query_dob)
    s_dob = clean_dob(sanction_dob)
    
    if not q_dob or not s_dob:
        return False
    
    return q_dob == s_dob

def calculate_nationality_match(query_nat: str, sanction_nat: str) -> bool:
    """Check if nationalities match"""
    if not query_nat or not sanction_nat:
        return False
    
    q_nat = query_nat.upper().strip()
    s_nat = sanction_nat.upper().strip()
    
    # Direct match
    if q_nat == s_nat:
        return True
    
    # Check if one contains the other
    if q_nat in s_nat or s_nat in q_nat:
        return True
    
    return False

def screen_name(full_name: str, date_of_birth: str = None, nationality: str = None, 
                id_number: str = None) -> Dict[str, Any]:
    """Screen a name against the sanctions database with conservative scoring"""
    
    if not full_name:
        return {
            "query_name": "",
            "total_matches": 0,
            "high_risk_count": 0,
            "medium_risk_count": 0,
            "low_risk_count": 0,
            "matches": []
        }
    
    query_name = full_name.strip()
    sanctions = get_all_sanctions()
    
    matches = []
    
    for sanction in sanctions:
        # Calculate name score
        name_score = calculate_name_score(query_name, sanction["full_name"])
        
        if name_score == 0:
            continue
        
        # Calculate field matches
        dob_match = calculate_dob_match(date_of_birth, sanction.get("date_of_birth"))
        nat_match = calculate_nationality_match(nationality, sanction.get("nationality"))
        id_match = calculate_id_match(id_number, sanction.get("id_number"))
        
        # Calculate final score based on matching fields
        # Conservative scoring: require DOB OR ID for high scores
        has_dob_or_id = dob_match or id_match
        
        if has_dob_or_id:
            # Full score when DOB or ID matches
            final_score = name_score
        else:
            # Reduced to 40% when neither DOB nor ID available
            final_score = 40.0
        
        # Determine risk level
        if final_score >= HIGH_RISK_THRESHOLD:
            risk_level = "HIGH"
        elif final_score >= MEDIUM_RISK_THRESHOLD:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        # Only include matches above threshold
        if final_score >= MATCH_THRESHOLD:
            matched_fields = ["name"]
            if dob_match:
                matched_fields.append("dob")
            if nat_match:
                matched_fields.append("nationality")
            if id_match:
                matched_fields.append("id")
            
            matches.append({
                "sanction_id": sanction["id"],
                "full_name": sanction["full_name"],
                "match_score": round(final_score, 1),
                "name_score": round(name_score, 1),
                "dob_match": dob_match,
                "nationality_match": nat_match,
                "id_match": id_match,
                "risk_level": risk_level,
                "source": sanction["source"],
                "matched_fields": matched_fields,
                "is_exact_match": name_score == 100.0 and has_dob_or_id
            })
    
    # Sort by score descending
    matches.sort(key=lambda x: x["match_score"], reverse=True)
    
    # Count risk levels
    high_risk = sum(1 for m in matches if m["risk_level"] == "HIGH")
    medium_risk = sum(1 for m in matches if m["risk_level"] == "MEDIUM")
    low_risk = sum(1 for m in matches if m["risk_level"] == "LOW")
    
    return {
        "query_name": query_name,
        "total_matches": len(matches),
        "high_risk_count": high_risk,
        "medium_risk_count": medium_risk,
        "low_risk_count": low_risk,
        "matches": matches
    }

# ============================================================
# XML PARSING
# ============================================================

def parse_moha_xml(xml_content: str) -> List[Dict]:
    """Parse MOHA sanctions list XML"""
    records = []
    
    try:
        # Clean up malformed XML - the MOHA file has </xmlResponse> at end but no opening tag
        # Remove any existing xmlResponse tags
        import re
        xml_content = re.sub(r'</?xmlResponse>', '', xml_content)
        
        # Add proper XML wrapper
        xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xmlResponse>
''' + xml_content.strip() + '''
</xmlResponse>'''
        
        root = ET.fromstring(xml_content)
        
        # Try MOHA custom format: <entry><field name="(3) Name">...</field></entry>
        entries = root.findall(".//entry")
        if entries:
            for entry in entries:
                name = None
                dob = None
                nationality = None
                id_number = None
                
                for field in entry.findall(".//field"):
                    field_name = field.get("name", "")
                    field_value = field.text if field.text else ""
                    
                    if field_name == "(3) Name":
                        name = field_value
                    elif field_name == "(6) Date of Birth":
                        dob = field_value
                    elif field_name == "(9) Nationality":
                        nationality = field_value
                    elif field_name == "(10) Passport Number":
                        id_number = field_value
                    elif field_name == "(11) Identification Card Number":
                        if not id_number:
                            id_number = field_value
                
                if name:
                    records.append({
                        "full_name": name,
                        "date_of_birth": clean_dob(dob) if dob else None,
                        "nationality": nationality,
                        "id_number": id_number,
                        "source": "MOHA",
                        "list_type": "MOHA"
                    })
            return records
        
        # Try standard XML structures
        for person in root.findall(".//PERSON") + root.findall(".//Individual") + root.findall(".//record"):
            name = None
            dob = None
            nationality = None
            id_number = None
            
            # Try various tag names
            for name_tag in ["NAME", "Name", "FullName", "full_name", "nama", "Nama"]:
                name_elem = person.find(name_tag)
                if name_elem is not None:
                    name = name_elem.text
                    break
            
            for dob_tag in ["DOB", "DateOfBirth", "date_of_birth", "TarikhLahir"]:
                dob_elem = person.find(dob_tag)
                if dob_elem is not None:
                    dob = dob_elem.text
                    break
            
            for nat_tag in ["NATIONALITY", "Nationality", "nationality", "Kerakyatan"]:
                nat_elem = person.find(nat_tag)
                if nat_elem is not None:
                    nationality = nat_elem.text
                    break
            
            for id_tag in ["ID", "IDNumber", "id_number", "NoPengenalan", "Passport"]:
                id_elem = person.find(id_tag)
                if id_elem is not None:
                    id_number = id_elem.text
                    break
            
            if name:
                records.append({
                    "full_name": name,
                    "date_of_birth": clean_dob(dob) if dob else None,
                    "nationality": nationality,
                    "id_number": id_number,
                    "source": "MOHA",
                    "list_type": "MOHA"
                })
        
        # If no structured format, try to parse as text
        if not records:
            for person in root.findall(".//") + root:
                text = person.text or ""
                if len(text) > 10 and any(c.isalpha() for c in text):
                    records.append({
                        "full_name": text.strip(),
                        "date_of_birth": None,
                        "nationality": None,
                        "id_number": None,
                        "source": "MOHA",
                        "list_type": "MOHA"
                    })
    
    except Exception as e:
        pass  # Silent fail
    
    return records

def fetch_moha_list() -> List[Dict]:
    """Fetch MOHA sanctions list from URL"""
    try:
        response = requests.get(MOHA_URL, timeout=30)
        response.raise_for_status()
        return parse_moha_xml(response.text)
    except Exception as e:
        st.error(f"Error fetching MOHA list: {e}")
        return []

# ============================================================
# UI FUNCTIONS
# ============================================================

def show_match_alert(matches: List[Dict]):
    """Show alert for matches found"""
    if not matches:
        return
    
    # Check for 100% matches
    exact_matches = [m for m in matches if m.get("is_exact_match") and m["match_score"] == 100]
    if exact_matches:
        st.error("🚨 **100% MATCH FOUND - HIGH RISK** 🚨")
        for match in exact_matches:
            st.error(f"• {match['full_name']} - {match['source']}")
    
    # Check for HIGH risk
    high_risk = [m for m in matches if m["risk_level"] == "HIGH" and m["match_score"] < 100]
    if high_risk:
        st.warning(f"⚠️ **{len(high_risk)} HIGH Risk Match(es) Found**")
        for match in high_risk:
            st.warning(f"• {match['full_name']} ({match['match_score']}% match) - {match['source']}")

def render_screening_form():
    """Render the main screening form"""
    st.header("🔍 Customer Screening")
    
    # Quick stats
    st.subheader("📊 Quick Stats")
    stats = get_sanction_count()
    
    col_stat1, col_stat2, col_stat3 = st.columns(3)
    with col_stat1:
        st.metric("Total Records", stats["total"])
    
    # Show UN and MOHA counts (use exact source names)
    un_count = stats["sources"].get("UN", 0)
    moha_count = stats["sources"].get("MOHA", 0)
    
    # Debug: show all sources
    if stats["sources"]:
        st.caption(f"Sources: {stats['sources']}")
    
    with col_stat2:
        st.metric("UN Sanction List", un_count)
    with col_stat3:
        st.metric("MOHA List", moha_count)
    
    st.divider()
    
    with st.form("screening_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            full_name = st.text_input("Full Name *", placeholder="Enter customer full name")
            date_of_birth = st.text_input("Date of Birth", placeholder="DD-MM-YYYY")
        
        with col2:
            nationality = st.text_input("Nationality", placeholder="e.g., MALAYSIA")
            id_number = st.text_input("ID/Passport Number", placeholder="e.g., A12345678")
        
        submitted = st.form_submit_button("🔍 Screen", use_container_width=True)
    
    if submitted and full_name:
        with st.spinner("Screening..."):
            result = screen_name(full_name, date_of_birth, nationality, id_number)
            
            # Show alerts
            show_match_alert(result["matches"])
            
            # Display results
            if result["matches"]:
                st.subheader(f"📊 Results: {result['total_matches']} Match(es) Found")
                
                # Stats
                stats_col1, stats_col2, stats_col3 = st.columns(3)
                stats_col1.metric("High Risk", result["high_risk_count"], delta_color="inverse")
                stats_col2.metric("Medium Risk", result["medium_risk_count"])
                stats_col3.metric("Low Risk", result["low_risk_count"])
                
                # Detailed results
                for match in result["matches"]:
                    with st.expander(f"{match['full_name']} - {match['match_score']}% ({match['risk_level']})"):
                        st.write(f"**Source:** {match['source']}")
                        st.write(f"**Match Score:** {match['match_score']}%")
                        st.write(f"**Name Score:** {match['name_score']}%")
                        st.write(f"**Risk Level:** {match['risk_level']}")
                        st.write(f"**Matched Fields:** {', '.join(match['matched_fields'])}")
                        if match.get("date_of_birth"):
                            st.write(f"**DOB:** {match['date_of_birth']}")
                        if match.get("nationality"):
                            st.write(f"**Nationality:** {match['nationality']}")
                        if match.get("id_number"):
                            st.write(f"**ID:** {match['id_number']}")
            else:
                st.success("✅ No matches found - Customer cleared")
    
    return submitted

def render_database_management():
    """Render database management section"""
    st.header("📂 Database Management")
    
    # Stats
    stats = get_sanction_count()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records", stats["total"])
    col2.metric("Sources", len(stats["sources"]))
    
    with col3:
        if st.button("🔄 Refresh Stats"):
            st.rerun()
    
    # Show sources breakdown
    if stats["sources"]:
        st.write("**By Source:**")
        for source, count in stats["sources"].items():
            st.write(f"  - {source}: {count}")
    
    st.divider()
    
    # Upload XML
    st.subheader("📤 Upload XML")
    
    tab1, tab2 = st.tabs(["Upload XML File", "Fetch from URL"])
    
    with tab1:
        uploaded_file = st.file_uploader("Choose XML file", type=["xml"])
        if uploaded_file:
            if st.button("Process XML File"):
                try:
                    content = uploaded_file.getvalue().decode("utf-8")
                    records = parse_moha_xml(content)
                    if records:
                        added = add_bulk_sanctions(records)
                        st.success(f"✅ Added {added} new records")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.warning("No records found in XML")
                except Exception as e:
                    st.error(f"Error: {e}")
    
    with tab2:
        st.write("Fetch MOHA sanctions list from official source")
        if st.button("Fetch MOHA List"):
            with st.spinner("Fetching..."):
                records = fetch_moha_list()
                if records:
                    added = add_bulk_sanctions(records)
                    st.success(f"✅ Added/found {added} records")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to fetch MOHA list")
    
    st.divider()
    
    # Manual entry
    st.subheader("➕ Add Manual Entry")
    
    with st.form("manual_entry"):
        m_col1, m_col2 = st.columns(2)
        with m_col1:
            m_name = st.text_input("Full Name")
            m_dob = st.text_input("Date of Birth")
        with m_col2:
            m_nat = st.text_input("Nationality")
            m_id = st.text_input("ID Number")
        
        if st.form_submit_button("Add Record"):
            if m_name:
                if add_sanction(m_name, m_dob, m_nat, m_id):
                    st.success("✅ Record added")
                    time.sleep(1)
                    st.rerun()
            else:
                st.warning("Name is required")
    
    st.divider()
    
    # View/Delete records
    st.subheader("📋 View Records")
    
    if stats["total"] > 0:
        df = pd.DataFrame(get_all_sanctions())
        st.dataframe(df[["id", "full_name", "date_of_birth", "nationality", "source"]], use_container_width=True)
        
        # Delete by ID
        with st.expander("Delete Record"):
            del_id = st.number_input("Record ID to delete", min_value=1, step=1)
            if st.button("Delete"):
                if delete_sanction(int(del_id)):
                    st.success("✅ Record deleted")
                    time.sleep(1)
                    st.rerun()
        
        # Clear all
        with st.expander("⚠️ Danger Zone"):
            st.warning("This will delete ALL sanctions records!")
            if st.button("Clear All Records", type="primary"):
                if clear_all_sanctions():
                    st.success("✅ All records cleared")
                    time.sleep(1)
                    st.rerun()
    else:
        st.info("No records in database")

def render_settings():
    """Render settings section"""
    st.header("⚙️ Settings")
    
    st.write("**Current Configuration:**")
    st.write(f"- Match Threshold: {MATCH_THRESHOLD}%")
    st.write(f"- High Risk Threshold: {HIGH_RISK_THRESHOLD}%")
    st.write(f"- Medium Risk Threshold: {MEDIUM_RISK_THRESHOLD}%")
    st.write(f"- MOHA URL: {MOHA_URL}")
    st.write(f"- UN URL: {UN_URL}")
    
    st.info("💡 For Streamlit Cloud, the database is stored in the app's session. Data will persist during the session but may reset on redeployment.")

# ============================================================
# MAIN APP
# ============================================================

def main():
    """Main Streamlit app"""
    
    # Initialize database
    init_database()
    
    # Auto-load XML files if database is empty
    loaded = auto_load_xml_files()
    if loaded["moha"] > 0 or loaded["un"] > 0:
        st.toast(f"Loaded {loaded['moha']} MOHA records and {loaded['un']} UN records from XML files")
    
    # Sidebar navigation
    st.sidebar.title("🛡️ Sanctions Screening")
    st.sidebar.image("https://img.icons8.com/ios-filled/100/shield.png", width=50)
    
    page = st.sidebar.radio("Navigate", ["Screening", "Database", "Settings"])
    
    if page == "Screening":
        render_screening_form()
    elif page == "Database":
        render_database_management()
    elif page == "Settings":
        render_settings()

if __name__ == "__main__":
    main()
