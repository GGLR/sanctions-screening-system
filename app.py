"""
Streamlit Web UI for Sanctions List Screening System
"""

import streamlit as st
import json
import time
from datetime import datetime
from pathlib import Path

# Try to import config, use defaults if not available
try:
    import config
except:
    class Config:
        FUZZY_MATCH_THRESHOLD = 70
        DATABASE_PATH = Path("data/sanctions.db")
        ADMIN_PASSWORD = "selangor@786"
        PASSWORD_PROTECTION_ENABLED = True
    config = Config()

# Try to import database and matching_engine, set to None if not available
try:
    import database
except:
    database = None

try:
    import matching_engine
except:
    matching_engine = None


# Configure page
st.set_page_config(
    page_title="Sanctions Screening System",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Branch options
BRANCHES = ["Midvalley", "Chowkit", "Klang", "Lot 10", "Summit", "Pavilion", "PBJ"]


# Initialize database and matching engine
@st.cache_resource
def get_db():
    """Get database connection"""
    if database is None:
        return None
    try:
        return database.get_database()
    except Exception as e:
        st.error(f"Database error: {e}")
        return None

@st.cache_resource
def get_engine():
    """Get matching engine"""
    if matching_engine is None:
        return None
    try:
        return matching_engine.get_matching_engine()
    except Exception as e:
        st.error(f"Matching engine error: {e}")
        return None


# Database path finder
def find_db_path():
    """Find the database file"""
    import os
    paths = [
        "data/sanctions.db",
        "sanctions.db",
        "/mount/src/sanctions-screening-system/sanctions.db",
        "/mount/src/sanctions-screening-system/data/sanctions.db",
    ]
    for p in paths:
        if os.path.exists(p):
            return Path(p)
    return None


def save_screening_log(branch: str, customer_name: str, total_matches: int, high_risk: int, medium_risk: int, low_risk: int, risk_level: str, exact_match: bool = False):
    """Save screening result to database"""
    import sqlite3
    import os
    from datetime import datetime
    
    db_path = find_db_path()
    if db_path is None:
        return False
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Create screening_log table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS screening_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                branch TEXT NOT NULL,
                customer_name TEXT NOT NULL,
                total_matches INTEGER,
                high_risk INTEGER,
                medium_risk INTEGER,
                low_risk INTEGER,
                risk_level TEXT,
                exact_match INTEGER DEFAULT 0,
                screened_at TEXT NOT NULL
            )
        ''')
        
        now = datetime.now().isoformat()
        cursor.execute('''
            INSERT INTO screening_log (branch, customer_name, total_matches, high_risk, medium_risk, low_risk, risk_level, exact_match, screened_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (branch, customer_name, total_matches, high_risk, medium_risk, low_risk, risk_level, 1 if exact_match else 0, now))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error saving log: {e}")
        return False


def get_screening_log(limit: int = 100):
    """Get screening log from database"""
    import sqlite3
    from datetime import datetime
    
    db_path = find_db_path()
    if db_path is None:
        return []
    
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS screening_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                branch TEXT NOT NULL,
                customer_name TEXT NOT NULL,
                total_matches INTEGER,
                high_risk INTEGER,
                medium_risk INTEGER,
                low_risk INTEGER,
                risk_level TEXT,
                exact_match INTEGER DEFAULT 0,
                screened_at TEXT NOT NULL
            )
        ''')
        
        cursor.execute('SELECT * FROM screening_log ORDER BY id DESC LIMIT ?', (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except:
        return []


# Password protection functions
def admin_login_sidebar():
    """Handle admin login in sidebar"""
    if not config.PASSWORD_PROTECTION_ENABLED:
        return True
    
    if 'admin_authenticated' not in st.session_state:
        st.session_state.admin_authenticated = False
    
    if not st.session_state.admin_authenticated:
        with st.sidebar:
            st.divider()
            st.markdown("### 🔐 Admin Access")
            password = st.text_input("Admin Password", type="password", key="admin_pass")
            
            if st.button("🔑 Login", key="login_btn"):
                if password == config.ADMIN_PASSWORD:
                    st.session_state.admin_authenticated = True
                    st.rerun()
                else:
                    st.error("Incorrect password")
            
            st.info("Password required to access screening log and settings")
        return False
    else:
        with st.sidebar:
            st.divider()
            col1, col2 = st.columns([2, 1])
            with col1:
                st.success("✅ Admin Access")
            with col2:
                if st.button("🚪", key="logout_btn", help="Logout"):
                    st.session_state.admin_authenticated = False
                    st.rerun()
        return True


def show_login_form():
    """Show password login form"""
    st.markdown("### 🔒 Admin Authentication Required")
    
    password = st.text_input("Enter Admin Password", type="password")
    
    if st.button("Login", type="primary"):
        if password == config.ADMIN_PASSWORD:
            st.session_state.admin_authenticated = True
            st.rerun()
        else:
            st.error("❌ Incorrect password")
    
    return False


# API replacement - direct database calls
def call_api(endpoint: str, method: str = "GET", data: dict = None, files: dict = None):
    """Make local function calls instead of API"""
    import sqlite3
    import os
    from pathlib import Path
    
    db = get_db()
    engine = get_engine()
    
    # Check for database and get stats
    if endpoint == "/api/statistics/official":
        db_path = find_db_path()
        
        # Debug output
        st.write("Debug - Checking database paths:")
        for p in ["data/sanctions.db", "sanctions.db", "/mount/src/sanctions-screening-system/sanctions.db"]:
            if os.path.exists(p):
                conn = sqlite3.connect(p)
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM sanctions')
                count = cursor.fetchone()[0]
                st.write(f"  {p}: {count} records")
                conn.close()
        
        if db_path and os.path.exists(db_path):
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute('SELECT source, COUNT(*) FROM sanctions GROUP BY source')
            by_source = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.execute('SELECT COUNT(*) FROM sanctions')
            total = cursor.fetchone()[0]
            conn.close()
        else:
            by_source = {}
            total = 0
        
        filtered = {}
        for k, v in by_source.items():
            if "UN" in k.upper():
                filtered["UN Sanction List"] = v
            elif "MOHA" in k.upper() or "MALAYSIA" in k.upper():
                filtered["MOHA List"] = v
            else:
                filtered[f"{k}"] = v
        
        return {"total_records": total, "by_source": filtered}
    
    # Screen customer
    if endpoint == "/api/screen" and method == "POST":
        full_name = data.get("full_name", "")
        dob = data.get("date_of_birth", "")
        nationality = data.get("nationality", "")
        id_number = data.get("id_number", "")
        
        # Get from database directly
        db_path = find_db_path()
        if db_path is None or not os.path.exists(db_path):
            return {"matches": [], "total_matches": 0, "high_risk_count": 0, "medium_risk_count": 0, "low_risk_count": 0}
        
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM sanctions WHERE is_active = 1')
        rows = cursor.fetchall()
        sanctions = [dict(row) for row in rows]
        conn.close()
        
        if not sanctions:
            return {"matches": [], "total_matches": 0, "high_risk_count": 0, "medium_risk_count": 0, "low_risk_count": 0}
        
        # Use rapidfuzz for matching
        try:
            from rapidfuzz import fuzz
        except:
            from difflib import SequenceMatcher
            def fuzz_ratio(a, b):
                return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100
        
        threshold = getattr(config, 'FUZZY_MATCH_THRESHOLD', 70)
        matches_list = []
        
        for sanction in sanctions:
            name_score = fuzz.ratio(full_name.lower(), sanction.get('full_name', '').lower())
            
            if name_score >= threshold:
                # Check which fields matched
                matched_fields = []
                dob_match = False
                nationality_match = False
                id_match = False
                
                # Name match (always)
                matched_fields.append("name")
                
                # Check DOB
                if dob and sanction.get('date_of_birth'):
                    db_dob = sanction.get('date_of_birth', '')
                    # Try various date formats
                    if dob in db_dob or db_dob in dob:
                        dob_match = True
                        matched_fields.append("date of birth")
                    # Also check partial match
                    dob_parts = dob.replace('-', '').replace('/', '')
                    db_parts = db_dob.replace('-', '').replace('/', '')
                    if dob_parts and db_parts and (dob_parts in db_parts or db_parts in dob_parts):
                        dob_match = True
                        matched_fields.append("date of birth")
                
                # Check Nationality
                if nationality and sanction.get('nationality'):
                    db_nat = sanction.get('nationality', '').lower()
                    if nationality.lower() in db_nat or db_nat in nationality.lower():
                        nationality_match = True
                        matched_fields.append("nationality")
                
                # Check ID
                if id_number and sanction.get('id_number'):
                    db_id = sanction.get('id_number', '').replace(' ', '')
                    check_id = id_number.replace(' ', '')
                    if check_id and (check_id in db_id or db_id in check_id):
                        id_match = True
                        matched_fields.append("ID number")
                
                # Determine risk level based on exact match criteria above
                if is_exact:
                    risk_level = "HIGH"
                elif name_score >= 85:
                    risk_level = "HIGH"
                elif name_score >= 70:
                    risk_level = "MEDIUM"
                else:
                    risk_level = "LOW"
                
                # 100% matched - specific combinations only:
                # 100% name + DOB
                # 100% name + DOB + nationality
                # 100% name + DOB + nationality + ID
                # DOB + nationality + ID
                # DOB + ID
                
                exact_1 = name_score == 100 and dob_match  # 100% name + DOB
                exact_2 = name_score == 100 and dob_match and nationality_match  # 100% name + DOB + nationality
                exact_3 = name_score == 100 and dob_match and nationality_match and id_match  # 100% name + DOB + nationality + ID
                exact_4 = dob_match and nationality_match and not name_score == 100  # DOB + nationality (no 100% name)
                exact_5 = dob_match and id_match and not name_score == 100  # DOB + ID (no 100% name)
                
                is_exact = exact_1 or exact_2 or exact_3 or exact_4 or exact_5
                
                # For tracking: count fields matched
                fields_matched = sum([dob_match, nationality_match, id_match])
                
                matches_list.append({
                    "sanction_id": sanction.get('id'),
                    "full_name": sanction.get('full_name'),
                    "match_score": name_score,
                    "name_score": name_score,
                    "dob_match": dob_match,
                    "nationality_match": nationality_match,
                    "id_match": id_match,
                    "risk_level": risk_level,
                    "source": sanction.get('source'),
                    "matched_fields": matched_fields,
                    "is_exact_match": is_exact,
                    # Also store the matched values for display
                    "matched_dob": sanction.get('date_of_birth') if dob_match else None,
                    "matched_nationality": sanction.get('nationality') if nationality_match else None,
                    "matched_id": sanction.get('id_number') if id_match else None,
                })
        
        matches_list.sort(key=lambda x: x['match_score'], reverse=True)
        
        high_risk = sum(1 for m in matches_list if m.get("risk_level") == "HIGH")
        medium_risk = sum(1 for m in matches_list if m.get("risk_level") == "MEDIUM")
        low_risk = sum(1 for m in matches_list if m.get("risk_level") == "LOW")
        
        return {
            "matches": matches_list,
            "total_matches": len(matches_list),
            "high_risk_count": high_risk,
            "medium_risk_count": medium_risk,
            "low_risk_count": low_risk
        }
    
    # Refresh from local files
    if endpoint == "/api/refresh/local" and method == "POST":
        import xml.etree.ElementTree as ET
        import hashlib
        
        results = {}
        
        # Find XML files
        search_paths = [".", os.getcwd(), "/mount/src/sanctions-screening-system", str(Path(__file__).parent)]
        
        xml_files = {
            'moha_sanctions_list.xml': 'MOHA_MALAYSIA',
            'un_sanctions_list.xml': 'UN_LIST'
        }
        
        found = {}
        for base in search_paths:
            for xml_file, source in xml_files.items():
                if source not in found:
                    test_path = os.path.join(base, xml_file)
                    if os.path.exists(test_path):
                        found[source] = (test_path, source)
        
        st.write("Debug - Found XML files:", found)
        
        db_path = find_db_path()
        if db_path is None:
            # Create new database
            db_path = Path("data/sanctions.db")
            db_path.parent.mkdir(exist_ok=True)
        
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
        
        for source, (filepath, source_name) in found.items():
            try:
                st.write(f"Parsing {filepath}...")
                
                # Fix malformed XML
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                content = content.replace('</xmlResponse>', '')
                
                root = ET.fromstring(content)
                records = []
                now = datetime.now().isoformat()
                
                # Try MOHA format
                for entry in root.findall('.//entry'):
                    full_name = dob = nationality = id_number = ''
                    
                    for field in entry.findall('field'):
                        name_attr = field.get('name', '')
                        value = field.text or ''
                        
                        if 'Name' in name_attr and not full_name:
                            full_name = value
                        elif 'Date of Birth' in name_attr and not dob:
                            dob = value
                        elif 'Nationality' in name_attr and not nationality:
                            nationality = value
                        elif ('Passport' in name_attr or 'ID' in name_attr) and not id_number:
                            id_number = value
                    
                    if full_name:
                        records.append({
                            'full_name': full_name,
                            'date_of_birth': dob,
                            'nationality': nationality,
                            'id_number': id_number,
                            'source': source_name
                        })
                
                # Try UN format
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
                    
                    dob_elem = entry.find('.//DATE_OF_BIRTH')
                    dob = dob_elem.text if dob_elem is not None and dob_elem.text else ''
                    
                    nat_elem = entry.find('.//NATIONALITY')
                    nationality = nat_elem.text if nat_elem is not None and nat_elem.text else ''
                    
                    id_elem = entry.find('.//NUMBER')
                    id_number = id_elem.text if id_elem is not None and id_elem.text else ''
                    
                    records.append({
                        'full_name': full_name,
                        'date_of_birth': dob,
                        'nationality': nationality,
                        'id_number': id_number,
                        'source': source_name
                    })
                
                st.write(f"Found {len(records)} records")
                
                # Insert records
                added = 0
                skipped = 0
                for record in records:
                    name = record.get('full_name', '')
                    dob = record.get('date_of_birth')
                    id_num = record.get('id_number')
                    unique_hash = hashlib.md5(f"{name}|{dob}|{id_num}".encode()).hexdigest()
                    
                    try:
                        cursor.execute('''
                            INSERT INTO sanctions (unique_hash, full_name, date_of_birth, nationality, id_number, source, created_at, updated_at, is_active)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                        ''', (unique_hash, name, dob, record.get('nationality'), id_num, source_name, now, now))
                        added += 1
                    except:
                        skipped += 1
                
                conn.commit()
                results[source_name] = {"added": added, "skipped": skipped}
                st.write(f"Added {added}, skipped {skipped}")
                
            except Exception as e:
                st.write(f"Error: {str(e)}")
                results[source_name] = {"error": str(e)}
        
        conn.close()
        return {"success": True, "results": results}
    
    return None


def check_api_connection():
    """Check if database is available"""
    db_path = find_db_path()
    if db_path:
        import sqlite3
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM sanctions')
            count = cursor.fetchone()[0]
            conn.close()
            return count > 0
        except:
            return False
    return False


def main():
    """Main application"""
    
    # Custom CSS
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E3A5F;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2C5282;
        margin-bottom: 0.5rem;
    }
    .risk-high { color: #E53E3E; font-weight: bold; }
    .risk-medium { color: #DD6B20; font-weight: bold; }
    .risk-low { color: #38A169; font-weight: bold; }
    .stButton>button { width: 100%; }
    </style>
    """, unsafe_allow_html=True)
    
    # Sidebar - check admin authentication
    admin_authenticated = admin_login_sidebar()
    
    with st.sidebar:
        st.title("🛡️ Sanctions Screening")
        
        # API Status
        if check_api_connection():
            st.success("✅ Database Connected")
        else:
            st.warning("⚠️ No data - Click Refresh")
        
        st.divider()
        
        # Navigation
        page = st.radio(
            "Navigation",
            ["🔍 Customer Screening", "📋 Sanctions Database", "📤 Update Lists", "📊 Statistics", "📋 Screening Log"]
        )
        
        st.divider()
        
        # Quick stats - always visible
        st.subheader("Quick Stats")
        stats = call_api("/api/statistics/official")
        if stats:
            by_source = stats.get("by_source", {})
            st.metric("Total Records", stats.get("total_records", 0))
            
            un_count = by_source.get("UN Sanction List", 0)
            moha_count = by_source.get("MOHA List", 0)
            
            col_stats1, col_stats2 = st.columns(2)
            with col_stats1:
                st.metric("UN Sanction List", un_count)
            with col_stats2:
                st.metric("MOHA List", moha_count)
        
        st.divider()
        
        # Refresh button
        if st.button("🔄 Refresh from Local Files", help="Reload data from local XML files"):
            with st.spinner("Refreshing from local files..."):
                result = call_api("/api/refresh/local", method="POST")
            
            if result:
                if result.get("success"):
                    st.success("✅ Refresh complete!")
                    st.rerun()
                else:
                    st.error(f"❌ Refresh failed")
    
    # Main content based on selected page
    if page == "🔍 Customer Screening":
        screening_page()
    elif page == "📋 Sanctions Database":
        database_page()
    elif page == "📤 Update Lists":
        update_page()
    elif page == "📊 Statistics":
        statistics_page()
    elif page == "📋 Screening Log":
        screening_log_page()


def screening_page():
    """Customer screening page"""
    st.markdown('<p class="main-header">🔍 Customer Screening</p>', unsafe_allow_html=True)
    
    st.markdown("Screen customers against sanctions lists using fuzzy matching")
    
    # Input form
    with st.form("screening_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            branch = st.selectbox("Branch *", BRANCHES, index=0)
            full_name = st.text_input("Full Name *", placeholder="Enter customer full name")
            date_of_birth = st.text_input("Date of Birth", placeholder="DD.MM.YYYY or YYYY-MM-DD")
        
        with col2:
            nationality = st.text_input("Nationality", placeholder="e.g., Malaysia, USA")
            id_number = st.text_input("ID/Passport Number", placeholder="Passport or ID number")
        
        include_below = st.checkbox("Include matches below threshold", 
                                    help="Show all potential matches, not just those above threshold")
        
        submitted = st.form_submit_button("🔍 Screen Customer", type="primary")
    
    if submitted and full_name:
        with st.spinner("Screening customer..."):
            result = call_api("/api/screen", method="POST", data={
                "full_name": full_name,
                "date_of_birth": date_of_birth if date_of_birth else None,
                "nationality": nationality if nationality else None,
                "id_number": id_number if id_number else None,
                "include_below_threshold": include_below
            })
        
        if result:
            # Check for 100% exact match
            matches = result.get("matches", [])
            exact_match = any(m.get("is_exact_match", False) for m in matches)
            
            # Save to screening log
            risk_level = "LOW"
            if exact_match:
                risk_level = "HIGH"
            elif result.get("high_risk_count", 0) > 0:
                risk_level = "HIGH"
            elif result.get("medium_risk_count", 0) > 0:
                risk_level = "MEDIUM"
            
            save_screening_log(
                branch=branch,
                customer_name=full_name,
                total_matches=result.get("total_matches", 0),
                high_risk=result.get("high_risk_count", 0),
                medium_risk=result.get("medium_risk_count", 0),
                low_risk=result.get("low_risk_count", 0),
                risk_level=risk_level,
                exact_match=exact_match
            )
            
            display_screening_results(result)
    
    elif submitted:
        st.warning("Please enter at least the customer's full name")


def display_screening_results(result: dict):
    """Display screening results"""
    st.divider()
    
    # Check for 100% exact matches
    matches = result.get("matches", [])
    exact_matches = [m for m in matches if m.get("is_exact_match", False)]
    
    # Show popup/alert for 100% exact matches
    if exact_matches:
        st.markdown("### 🚨 CRITICAL ALERT: 100% MATCH DETECTED 🚨")
        st.error("**Customer 100% matched. Do not proceed with the transaction and contact Compliance division.**")
        for match in exact_matches:
            st.write(f"- **{match['full_name']}** (Source: {match.get('source', 'N/A')})")
        st.markdown("---")
    
    # Summary
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Matches", result.get("total_matches", 0))
    with col2:
        st.markdown(f"**🔴 High Risk:** {result.get('high_risk_count', 0)}")
    with col3:
        st.markdown(f"**🟡 Medium Risk:** {result.get('medium_risk_count', 0)}")
    with col4:
        st.markdown(f"**🟢 Low Risk:** {result.get('low_risk_count', 0)}")
    
    if result.get("high_risk_count", 0) > 0 and not exact_matches:
        st.error("⚠️ ALERT: High risk matches detected! Please review immediately.")
    
    if matches:
        st.subheader("Match Results")
        
        for i, match in enumerate(matches):
            with st.expander(f"{match['full_name']} - {match['risk_level']} Risk (Score: {match['match_score']}%)", 
                           expanded=match['risk_level'] == "HIGH"):
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Name Score:** {match.get('name_score', 0)}%")
                    st.write(f"**Source:** {match.get('source', 'N/A')}")
                    
                    # Show matched fields
                    matched_fields = match.get('matched_fields', [])
                    st.write(f"**Matched Fields:** {', '.join(matched_fields)}")
                
                with col2:
                    risk_colors = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}
                    st.write(f"**Risk Level:** {risk_colors.get(match['risk_level'], '')} {match['risk_level']}")
                    st.write(f"**Match Score:** {match['match_score']}%")
                    
                    # Show which specific values matched
                    if match.get('dob_match'):
                        st.write(f"✅ DOB Match: {match.get('matched_dob', 'N/A')}")
                    else:
                        st.write(f"❌ DOB: {match.get('matched_dob', 'N/A')}")
                    
                    if match.get('nationality_match'):
                        st.write(f"✅ Nationality Match: {match.get('matched_nationality', 'N/A')}")
                    else:
                        st.write(f"❌ Nationality: {match.get('matched_nationality', 'N/A')}")
                    
                    if match.get('id_match'):
                        st.write(f"✅ ID Match: {match.get('matched_id', 'N/A')}")
                    else:
                        st.write(f"❌ ID: {match.get('matched_id', 'N/A')}")
    else:
        st.info("No matches found in the sanctions database.")


def database_page():
    """Sanctions database page"""
    st.markdown('<p class="main-header">📋 Sanctions Database</p>', unsafe_allow_html=True)
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["🔎 Search", "➕ Add Manual", "📜 Update History"])
    
    with tab1:
        st.subheader("Search Sanctions")
        
        search_name = st.text_input("Search by name", placeholder="Enter name to search")
        
        if search_name:
            st.info("Search feature - use Customer Screening for fuzzy matching")
    
    with tab2:
        st.subheader("Add Manual Entry")
        st.info("Add manual entry - login required")
    
    with tab3:
        st.subheader("Update History")
        
        if not admin_authenticated:
            show_login_form()
        else:
            st.success("Admin access granted")


def update_page():
    """Update sanctions lists page"""
    st.markdown('<p class="main-header">📤 Update Sanctions Lists</p>', unsafe_allow_html=True)
    
    # Current settings - password protected
    with st.expander("⚙️ Current Settings", expanded=False):
        if not admin_authenticated:
            st.warning("🔒 Admin password required to view settings")
            show_login_form()
        else:
            st.write("Settings panel (admin only)")


def screening_log_page():
    """Screening log page - password protected"""
    st.markdown('<p class="main-header">📋 Screening Log</p>', unsafe_allow_html=True)
    
    # Check admin authentication - ALWAYS require login
    if 'admin_authenticated' not in st.session_state:
        st.session_state.admin_authenticated = False
    
    if not st.session_state.admin_authenticated:
        st.warning("🔒 Please login to view Screening Log")
        password = st.text_input("Enter Admin Password", type="password", key="screening_log_pass")
        if st.button("Login", key="screening_log_login"):
            if password == config.ADMIN_PASSWORD:
                st.session_state.admin_authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password")
        return
    
    # Get screening log
    logs = get_screening_log(limit=100)
    
    if logs:
        import pandas as pd
        df = pd.DataFrame(logs)
        
        # Format the datetime
        df['screened_at'] = pd.to_datetime(df['screened_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Format exact match column
        if 'exact_match' in df.columns:
            df['100% Match'] = df['exact_match'].apply(lambda x: '✅ YES' if x == 1 else '❌ NO')
        
        st.dataframe(df, use_container_width=True)
        
        # Summary stats
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Screenings", len(logs))
        with col2:
            exact_count = sum(1 for l in logs if l.get('exact_match') == 1)
            st.metric("100% Match", exact_count)
        with col3:
            high_count = sum(1 for l in logs if l.get('risk_level') == 'HIGH')
            st.metric("High Risk", high_count)
        with col4:
            low_risk_count = sum(1 for l in logs if l.get('risk_level') == 'LOW')
            st.metric("Low Risk", low_risk_count)
    else:
        st.info("No screening records found.")


def statistics_page():
    """Statistics page"""
    st.markdown('<p class="main-header">📊 Statistics</p>', unsafe_allow_html=True)
    
    stats = call_api("/api/statistics/official")
    
    if stats:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Records", stats.get("total_records", 0))
        
        with col2:
            by_source = stats.get("by_source", {})
            un_count = by_source.get("UN Sanction List", 0)
            st.metric("UN Sanction List", un_count)
        
        with col3:
            moha_count = by_source.get("MOHA List", 0)
            st.metric("MOHA List", moha_count)


if __name__ == "__main__":
    main()
