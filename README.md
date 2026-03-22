# 🛡️ Sanctions List Screening System

A Money Services Business (MSB) Sanctions List Screening System with fuzzy matching for customer screening.

## 🌐 Streamlit Cloud Deployment

### Option 1: Deploy `app_cloud.py` (Recommended)

This version works standalone without FastAPI backend - perfect for Streamlit Cloud.

#### Step 1: Create GitHub Repository

1. Go to [GitHub](https://github.com) and sign in
2. Click **New Repository**
3. Name: `sanctions-screening-system`
4. Set to **Public** (free for Streamlit Cloud)
5. Click **Create Repository**

#### Step 2: Upload Files

Upload these files to your repository:

```
sanctions_screening/
├── app.py                 # Main Streamlit app
├── requirements.txt      # Dependencies
├── moha_sanctions_list.xml # MOHA Malaysia list (87 records)
├── un_sanctions_list.xml   # UN Sanctions list (549 records)
└── README.md              # This file
```

**Or use Git commands:**
```bash
# Clone your repo
git clone https://github.com/YOUR_USERNAME/sanctions-screening-system.git
cd sanctions-screening-system

# Copy the files
# (make sure to copy app_cloud.py to app.py or keep as is)

# Commit and push
git add .
git commit -m "Initial commit"
git push origin main
```

#### Step 3: Deploy to Streamlit Cloud

1. Go to [Streamlit Cloud](https://share.streamlit.io)
2. Sign in with GitHub
3. Click **New app**
4. Select your repository:
   - Repository: `YOUR_USERNAME/sanctions-screening-system`
   - Branch: `main`
   - Main file path: `sanctions_screening/app_cloud.py`
5. Click **Deploy**

#### Step 4: Configure

In Streamlit Cloud settings:
- **Python version**: 3.9 or 3.10
- **Requirements file**: `sanctions_screening/requirements_cloud.txt`

That's it! Your app will be live at `https://sanctions-screening-system.streamlit.app`

---

## 🔧 Local Development

### Running Locally

```bash
# Clone or navigate to project
cd sanctions_screening

# Install dependencies
pip install -r requirements_cloud.txt

# Run the app
streamlit run app_cloud.py
```

The app will open at http://localhost:8501

---

## 📋 Features

### Screening
- **Full Name** matching with fuzzy logic
- **Date of Birth** matching
- **Nationality** matching
- **ID/Passport** number matching

### Matching Rules (Conservative)
| Scenario | Score | Risk |
|----------|-------|------|
| Name + DOB match | 100% | HIGH |
| Name + ID match | 100% | HIGH |
| Name only (no DOB/ID) | 40% | LOW |

### Database
- **XML Upload**: Upload local XML files
- **Auto Fetch**: Fetch MOHA Malaysia list from official source
- **Manual Entry**: Add individual records

---

## 📊 Risk Levels

- **HIGH** (🔴): Match score ≥ 85%
- **MEDIUM** (🟡): Match score 70-84%
- **LOW** (🟢): Match score < 70%

---

## 🔒 Important Notes

1. **Data Persistence**: On Streamlit Cloud, database resets on each redeployment. Re-upload XML files after redeployment.

2. **Network Access**: The app needs internet access to fetch MOHA list from external URLs.

3. **Legal Compliance**: This is a screening tool only. Always verify matches manually and comply with local regulations.

---

## 📁 File Structure

```
sanctions_screening/
├── app_cloud.py           # Standalone Streamlit app (for cloud)
├── app.py                # Streamlit app with FastAPI backend (local)
├── api.py                # FastAPI backend
├── requirements_cloud.txt # Cloud deployment requirements
├── requirements.txt      # Local development requirements
├── moha_sanctions_list.xml # Sample MOHA data
└── data/
    └── sanctions.db      # SQLite database
```

---

## 🚀 Quick Start for Cloud

1. **Copy** `app_cloud.py` → rename to `app.py` in your repo
2. **Copy** `requirements_cloud.txt` → rename to `requirements.txt`
3. **Push** to GitHub
4. **Deploy** to Streamlit Cloud

Done! 🎉
