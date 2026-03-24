"""
Streamlit Web UI for Sanctions List Screening System
"""

import streamlit as st
import json
import time
from datetime import datetime
import config
import database
import matching_engine

# Configure page
st.set_page_config(
    page_title="Sanctions Screening System",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add fallback methods to database module
def _patch_database_module():
    """Add fallback methods to database module if missing"""
    import sqlite3
    
    # Get the singleton instance (call get_database to ensure it's created)
    db = database.get_database()
    if db is None:
        return None
    
    # Check if get_statistics method exists, if not add it
    if not hasattr(db, 'get_statistics'):
        def get_statistics_fallback(self):
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM sanctions WHERE is_active = 1')
            total = cursor.fetchone()[0]
            cursor.execute('SELECT source, COUNT(*) as count FROM sanctions WHERE is_active = 1 GROUP BY source')
            by_source = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.execute('SELECT nationality, COUNT(*) as count FROM sanctions WHERE is_active = 1 AND nationality IS NOT NULL GROUP BY nationality ORDER BY count DESC LIMIT 10')
            by_nationality = {row[0]: row[1] for row in cursor.fetchall()}
            conn.close()
            return {"total_records": total, "by_source": by_source, "by_nationality": by_nationality}
        db.get_statistics = lambda: get_statistics_fallback(db)
    
    # Check if get_all_sanctions method exists, if not add it
    if not hasattr(db, 'get_all_sanctions'):
        def get_all_sanctions_fallback(self, limit=1000, offset=0):
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM sanctions WHERE is_active = 1 LIMIT ? OFFSET ?', (limit, offset))
            rows = cursor.fetchall()
            conn.close()
            return [dict(row) for row in rows]
        db.get_all_sanctions = lambda limit=1000, offset=0: get_all_sanctions_fallback(db, limit, offset)
    
    return db

@st.cache_resource
def get_db():
    try:
        # Patch the database module first
        db = _patch_database_module()
        return db
    except Exception as e:
        st.error(f"Failed to initialize database: {e}")
        return None

@st.cache_resource
def get_engine():
    try:
        # Ensure database is patched first
        _patch_database_module()
        
        # Now get the matching engine
        engine = matching_engine.get_matching_engine()
        return engine
    except Exception as e:
        st.error(f"Failed to initialize matching engine: {e}")
        return None


def check_admin_auth():
    """Check if admin is authenticated"""
    if not config.PASSWORD_PROTECTION_ENABLED:
        return True
    
    # Initialize session state
    if 'admin_authenticated' not in st.session_state:
        st.session_state.admin_authenticated = False
    
    return st.session_state.admin_authenticated


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


def check_api_connection():
    """Check if database is available"""
    try:
        db = get_db()
        if db:
            stats = db.get_statistics()
            return stats is not None
        return False
    except:
        return False


def call_api(endpoint: str, method: str = "GET", data: dict = None, files: dict = None):
    """Make local function calls instead of API"""
    db = get_db()
    engine = get_engine()
    
    try:
        # Screen customer
        if endpoint == "/api/screen" and method == "POST":
            matches = engine.screen_customer(
                full_name=data.get("full_name"),
                dob=data.get("date_of_birth"),
                nationality=data.get("nationality"),
                id_number=data.get("id_number")
            )
            # Convert MatchResult objects to dicts
            matches_list = [m.to_dict() for m in matches]
            
            # Count risk levels
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
        
        # Search sanctions
        elif endpoint.startswith("/api/sanctions/search/"):
            name = endpoint.split("/api/sanctions/search/")[1]
            return db.search_by_name(name)
        
        # Get all sanctions
        elif endpoint == "/api/sanctions":
            return db.get_all_sanctions(limit=5000)
        
        # Add sanction
        elif endpoint == "/api/sanctions" and method == "POST":
            result = db.add_sanction(
                name=data.get("full_name"),
                dob=data.get("date_of_birth"),
                nationality=data.get("nationality"),
                id_number=data.get("id_number"),
                id_type=data.get("id_type"),
                source=data.get("source"),
                comments=data.get("comments")
            )
            return {"success": True, "message": "Sanction added successfully", "id": result}
        
        # Get statistics
        elif endpoint == "/api/statistics":
            return db.get_statistics()
        
        # Get official statistics (filtered)
        elif endpoint == "/api/statistics/official":
            stats = db.get_statistics()
            # Filter to only show UN and MOHA (also check for variations in naming)
            by_source = stats.get("by_source", {})
            # Match various possible source name formats
            filtered = {}
            for k, v in by_source.items():
                if "UN" in k.upper() or "UNITED NATIONS" in k.upper():
                    filtered["UN Sanction List"] = v
                elif "MOHA" in k.upper() or "MALAYSIA" in k.upper() or "MINISTRY OF HOME" in k.upper():
                    filtered["MOHA List"] = v
            return {"total_records": stats.get("total_records", 0), "by_source": filtered}
        
        # Get settings
        elif endpoint == "/api/settings":
            return {
                "FUZZY_MATCH_THRESHOLD": config.FUZZY_MATCH_THRESHOLD,
                "AUTO_UPDATE_ENABLED": config.AUTO_UPDATE_ENABLED,
                "SANCTIONS_URLS": config.SANCTIONS_URLS
            }
        
        # Get update history
        elif endpoint == "/api/history":
            return db.get_update_history()
        
        # Refresh from local files
        elif endpoint == "/api/refresh/local" and method == "POST":
            from xml_parser import XMLParser
            parser = XMLParser()
            results = {}
            for source, filepath in config.LOCAL_XML_FILES.items():
                if filepath.exists():
                    records = parser.parse_file(str(filepath), source)
                    result = db.add_sanctions_batch(records, source)
                    results[source] = result
            return {"success": True, "results": results}
        
        else:
            st.warning(f"Endpoint not implemented: {endpoint}")
            return None
            
    except Exception as e:
        st.error(f"⚠️ Error: {str(e)}")
        return None


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
    .risk-high {
        color: #E53E3E;
        font-weight: bold;
    }
    .risk-medium {
        color: #DD6B20;
        font-weight: bold;
    }
    .risk-low {
        color: #38A169;
        font-weight: bold;
    }
    .success-box {
        padding: 1rem;
        background-color: #C6F6D5;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .warning-box {
        padding: 1rem;
        background-color: #FEEBC8;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .stButton>button {
        width: 100%;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Sidebar - check admin authentication
    admin_authenticated = admin_login_sidebar()
    
    with st.sidebar:
        st.title("🛡️ Sanctions Screening")
        
        # API Status
        if check_api_connection():
            st.success("✅ API Connected")
        else:
            st.error("❌ API Offline")
        
        st.divider()
        
        # Navigation
        page = st.radio(
            "Navigation",
            ["🔍 Customer Screening", "📋 Sanctions Database", "📤 Update Lists", "📊 Statistics"]
        )
        
        st.divider()
        
        # Quick stats - Only UN and MOHA
        st.subheader("Quick Stats")
        stats = call_api("/api/statistics/official")
        if stats:
            by_source = stats.get("by_source", {})
            st.metric("Total Records", stats.get("total_records", 0))
            
            # Show UN and MOHA counts
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
                    for key, value in result.get("results", {}).items():
                        if isinstance(value, dict):
                            st.write(f"- {key}: Added {value.get('added', 0)}, Skipped {value.get('skipped', 0)}")
                        else:
                            st.write(f"- {key}: {value}")
                else:
                    st.error(f"❌ Refresh failed: {result.get('message')}")
    
    # Main content based on selected page
    if page == "🔍 Customer Screening":
        screening_page()
    elif page == "📋 Sanctions Database":
        database_page()
    elif page == "📤 Update Lists":
        update_page()
    elif page == "📊 Statistics":
        statistics_page()


def screening_page():
    """Customer screening page"""
    st.markdown('<p class="main-header">🔍 Customer Screening</p>', unsafe_allow_html=True)
    
    st.markdown("Screen customers against sanctions lists using fuzzy matching")
    
    # Input form
    with st.form("screening_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            full_name = st.text_input("Full Name *", placeholder="Enter customer full name")
            date_of_birth = st.text_input("Date of Birth", placeholder="YYYY-MM-DD")
        
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
        st.markdown("**Matched Person(s):**")
        for match in exact_matches:
            st.write(f"- **{match['full_name']}** (Source: {match.get('source', 'N/A')})")
            st.write(f"  - Match Score: {match['match_score']}%")
            if match.get('dob_match'):
                st.write("  - ✅ DOB Match")
            if match.get('nationality_match'):
                st.write("  - ✅ Nationality Match")
            if match.get('id_match'):
                st.write("  - ✅ ID Match")
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
    
    # Alert for high risk (only if no exact matches)
    if result.get("high_risk_count", 0) > 0 and not exact_matches:
        st.error("⚠️ ALERT: High risk matches detected! Please review immediately.")
    
    if matches:
        st.subheader("Match Results")
        
        for i, match in enumerate(matches):
            with st.expander(f"{match['full_name']} - {match['risk_level']} Risk", 
                           expanded=match['risk_level'] == "HIGH"):
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Match Score:** {match['match_score']}%")
                    st.write(f"**Name Score:** {match['name_score']}%")
                    st.write(f"**Source:** {match.get('source', 'N/A')}")
                
                with col2:
                    risk_colors = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}
                    st.write(f"**Risk Level:** {risk_colors.get(match['risk_level'], '')} {match['risk_level']}")
                    
                    if match.get("dob_match"):
                        st.write("✅ DOB Match")
                    if match.get("nationality_match"):
                        st.write("✅ Nationality Match")
                    if match.get("id_match"):
                        st.write("✅ ID Match")
                
                st.write(f"**Matched Fields:** {', '.join(match.get('matched_fields', []))}")
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
            results = call_api(f"/api/sanctions/search/{search_name}")
            
            if results:
                st.write(f"Found {len(results)} results:")
                
                for record in results:
                    with st.expander(f"{record.get('full_name', 'Unknown')}"):
                        st.write(f"**Name:** {record.get('full_name')}")
                        st.write(f"**DOB:** {record.get('date_of_birth', 'N/A')}")
                        st.write(f"**Nationality:** {record.get('nationality', 'N/A')}")
                        st.write(f"**ID:** {record.get('id_number', 'N/A')}")
                        st.write(f"**Source:** {record.get('source')}")
            else:
                st.info("No results found")
        
        # Show all
        if st.button("Show All Sanctions"):
            all_sanctions = call_api("/api/sanctions")
            
            if all_sanctions:
                st.write(f"Showing {len(all_sanctions)} records:")
                
                # Display as dataframe
                import pandas as pd
                df = pd.DataFrame(all_sanctions)
                st.dataframe(df, use_container_width=True)
    
    with tab2:
        st.subheader("Add Manual Entry")
        
        with st.form("add_manual"):
            name = st.text_input("Full Name *")
            dob = st.text_input("Date of Birth")
            nationality = st.text_input("Nationality")
            id_num = st.text_input("ID/Passport Number")
            id_type = st.selectbox("ID Type", ["ID", "PASSPORT", "OTHER"])
            source = st.selectbox("Source", ["MOHA", "UN"], help="Only MOHA and UN are allowed")
            comments = st.text_area("Comments")
            
            submit = st.form_submit_button("➕ Add Sanction")
            
            if submit and name:
                result = call_api("/api/sanctions", method="POST", data={
                    "full_name": name,
                    "date_of_birth": dob if dob else None,
                    "nationality": nationality if nationality else None,
                    "id_number": id_num if id_num else None,
                    "id_type": id_type,
                    "source": source,
                    "comments": comments if comments else None
                })
                
                if result:
                    if result.get("success"):
                        st.success(result.get("message"))
                    else:
                        st.warning(result.get("message"))
    
    with tab3:
        st.subheader("Update History")
        
        # Check admin authentication for screening log
        if not admin_authenticated:
            show_login_form()
        else:
            history = call_api("/api/history")
            
            if history:
                import pandas as pd
                df = pd.DataFrame(history)
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No update history available")


def update_page():
    """Update sanctions lists page"""
    st.markdown('<p class="main-header">📤 Update Sanctions Lists</p>', unsafe_allow_html=True)
    
    # Current settings - password protected
    with st.expander("⚙️ Current Settings", expanded=False):
        if not admin_authenticated:
            st.warning("🔒 Admin password required to view settings")
            show_login_form()
        else:
            settings = call_api("/api/settings")
            if settings:
                st.write(f"**Fuzzy Match Threshold:** {settings.get('FUZZY_MATCH_THRESHOLD')}%")
                st.write(f"**Auto Update Enabled:** {settings.get('AUTO_UPDATE_ENABLED')}")
                st.write("**URLs:**")
                for key, url in settings.get("SANCTIONS_URLS", {}).items():
                    st.code(url)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🇲🇾 MOHA Malaysia List")
        st.write("Update from Ministry of Home Affairs Malaysia sanctions list")
        
        if st.button("🔄 Update MOHA List", key="update_moha"):
            with st.spinner("Fetching and processing MOHA list..."):
                result = call_api("/api/update/moha", method="POST")
            
            if result:
                if result.get("success"):
                    st.success(f"✅ {result.get('message')}")
                    st.write(f"Records added: {result.get('records_added')}")
                    st.write(f"Records skipped (duplicates): {result.get('records_skipped')}")
                else:
                    st.error(f"❌ {result.get('message')}")
    
    with col2:
        st.subheader("🌐 UN Sanctions List")
        st.write("Update from United Nations consolidated sanctions list")
        
        if st.button("🔄 Update UN List", key="update_un"):
            with st.spinner("Fetching and processing UN list..."):
                result = call_api("/api/update/un", method="POST")
            
            if result:
                if result.get("success"):
                    st.success(f"✅ {result.get('message')}")
                    st.write(f"Records added: {result.get('records_added')}")
                    st.write(f"Records skipped (duplicates): {result.get('records_skipped')}")
                else:
                    st.error(f"❌ {result.get('message')}")
    
    st.divider()
    
    # Update all
    if st.button("🔄 Update All Lists", type="primary"):
        with st.spinner("Updating all lists..."):
            result = call_api("/api/update/all", method="POST")
        
        if result:
            st.success("✅ Update complete")
            for key, status in result.get("results", {}).items():
                st.write(f"- {key}: {status}")
    
    st.divider()
    
    # XML Upload
    st.subheader("📁 Upload Custom XML")
    
    with st.form("upload_xml"):
        uploaded_file = st.file_uploader("Choose XML file", type=["xml"])
        source_name = st.text_input("Source Name", value="CUSTOM_XML", 
                                     help="Identifier for this data source")
        
        submit = st.form_submit_button("📤 Upload XML")
        
        if submit and uploaded_file:
            with st.spinner("Processing XML file..."):
                files = {"file": uploaded_file}
                data = {"source": source_name}
                
                result = call_api("/api/upload/xml", method="POST", data=data, files=files)
            
            if result:
                if result.get("success"):
                    st.success(f"✅ {result.get('message')}")
                    st.write(f"Records added: {result.get('records_added')}")
                    st.write(f"Records skipped (duplicates): {result.get('records_skipped')}")
                else:
                    st.error(f"❌ {result.get('message')}")
        elif submit:
            st.warning("Please select an XML file to upload")


def statistics_page():
    """Statistics page"""
    st.markdown('<p class="main-header">📊 Statistics</p>', unsafe_allow_html=True)
    
    # Get statistics (official only)
    stats = call_api("/api/statistics/official")
    
    if stats:
        # Overview
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
        
        # By source chart (only UN and MOHA)
        if by_source:
            st.subheader("Records by Source (UN and MOHA Only)")
            
            import pandas as pd
            source_data = [{"Source": k, "Count": v} for k, v in by_source.items() if v > 0]
            source_df = pd.DataFrame(source_data)
            
            if not source_df.empty:
                st.bar_chart(source_df.set_index("Source"))
        
        # Get full statistics for nationality
        full_stats = call_api("/api/statistics")
        by_nationality = full_stats.get("by_nationality", {}) if full_stats else {}
        
        # By nationality chart
        if by_nationality:
            st.subheader("Top 10 Nationalities")
            
            nat_data = [{"Nationality": k, "Count": v} for k, v in 
                       list(by_nationality.items())[:10]]
            nat_df = pd.DataFrame(nat_data)
            
            st.bar_chart(nat_df.set_index("Nationality"))
        
        # Detailed table
        with st.expander("View Detailed Statistics"):
            st.write("### By Source")
            st.table(source_df)
            
            st.write("### By Nationality")
            nat_df_full = pd.DataFrame([
                {"Nationality": k, "Count": v} for k, v in by_nationality.items()
            ])
            st.table(nat_df_full)
        
    else:
        st.info("No statistics available")


# Run the app
if __name__ == "__main__":
    main()
