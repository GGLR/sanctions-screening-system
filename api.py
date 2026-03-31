"""
FastAPI Application for Sanctions List Screening System
"""

import logging
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import xml.etree.ElementTree as ET

import config
from database import get_database
from matching_engine import get_matching_engine
from xml_parser import fetch_and_parse, get_parser, parse_local_file

# Configure logging
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Sanctions List Screening System",
    description="MSB Sanctions Screening API with fuzzy matching",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response Models
class ScreeningRequest(BaseModel):
    """Customer screening request"""
    full_name: str = Field(..., min_length=1, description="Customer full name")
    date_of_birth: Optional[str] = Field(None, description="Date of birth (YYYY-MM-DD)")
    nationality: Optional[str] = Field(None, description="Nationality")
    id_number: Optional[str] = Field(None, description="ID/Passport number")
    include_below_threshold: bool = Field(False, description="Include matches below threshold")


class ScreeningResponse(BaseModel):
    """Screening response"""
    query_name: str
    total_matches: int
    high_risk_count: int
    medium_risk_count: int
    low_risk_count: int
    matches: List[Dict[str, Any]]


class SanctionRecord(BaseModel):
    """Sanction record"""
    id: int
    full_name: str
    date_of_birth: Optional[str]
    nationality: Optional[str]
    id_number: Optional[str]
    source: str


class UpdateResponse(BaseModel):
    """Update operation response"""
    success: bool
    message: str
    records_added: int = 0
    records_skipped: int = 0


class StatisticsResponse(BaseModel):
    """Database statistics response"""
    total_records: int
    by_source: Dict[str, int]
    by_nationality: Dict[str, int]


# Initialize services
db = get_database()
matching_engine = get_matching_engine()


# Health check endpoint
@app.get("/")
async def root():
    """Root endpoint - API health check"""
    return {
        "status": "healthy",
        "service": "Sanctions List Screening System",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "database": "connected",
        "threshold": config.FUZZY_MATCH_THRESHOLD
    }


# Screening endpoints
@app.post("/api/screen", response_model=ScreeningResponse)
async def screen_customer(request: ScreeningRequest):
    """
    Screen a customer against sanctions list
    
    Performs fuzzy matching on the customer's name and other details
    against the sanctions database.
    """
    try:
        results = matching_engine.screen_customer(
            full_name=request.full_name,
            dob=request.date_of_birth,
            nationality=request.nationality,
            id_number=request.id_number,
            include_below_threshold=request.include_below_threshold
        )
        
        # Count by risk level
        high_risk = sum(1 for r in results if r.risk_level == "HIGH")
        medium_risk = sum(1 for r in results if r.risk_level == "MEDIUM")
        low_risk = sum(1 for r in results if r.risk_level == "LOW")
        
        return ScreeningResponse(
            query_name=request.full_name,
            total_matches=len(results),
            high_risk_count=high_risk,
            medium_risk_count=medium_risk,
            low_risk_count=low_risk,
            matches=[r.to_dict() for r in results]
        )
        
    except Exception as e:
        logger.error(f"Error screening customer: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/screen/name/{name}", response_model=List[Dict])
async def screen_by_name(name: str, limit: int = Query(10, ge=1, le=100)):
    """
    Quick name-only screening (for autocomplete/typeahead)
    """
    try:
        results = matching_engine.screen_name_only(name=name, limit=limit)
        return results
    except Exception as e:
        logger.error(f"Error in name screening: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Database endpoints
@app.get("/api/sanctions", response_model=List[Dict])
async def get_sanctions(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    source: Optional[str] = Query(None)
):
    """Get all sanctions from database"""
    try:
        if source:
            sanctions = db.search_by_name(source, limit=limit)
        else:
            sanctions = db.get_all_sanctions(limit=limit, offset=offset)
        return sanctions
    except Exception as e:
        logger.error(f"Error getting sanctions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sanctions/{sanction_id}", response_model=Dict)
async def get_sanction(sanction_id: int):
    """Get a specific sanction by ID"""
    try:
        sanction = db.get_sanction_by_id(sanction_id)
        if not sanction:
            raise HTTPException(status_code=404, detail="Sanction not found")
        return sanction
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting sanction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sanctions/search/{name}", response_model=List[Dict])
async def search_sanctions(name: str, limit: int = Query(50, ge=1, le=200)):
    """Search sanctions by name"""
    try:
        results = db.search_by_name(name, limit=limit)
        return results
    except Exception as e:
        logger.error(f"Error searching sanctions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Statistics endpoint
@app.get("/api/statistics", response_model=StatisticsResponse)
async def get_statistics():
    """Get database statistics"""
    try:
        stats = db.get_statistics()
        return StatisticsResponse(**stats)
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Statistics endpoint (UN, MOHA, and PEP)
@app.get("/api/statistics/official", response_model=Dict)
async def get_official_statistics():
    """Get statistics for official sanctions lists (UN, MOHA, and PEP)"""
    try:
        stats = db.get_statistics()
        
        # Include UN, MOHA, and PEP sources
        by_source = stats.get("by_source", {})
        filtered_source = {
            "UN Sanction List": by_source.get("UN_LIST", 0) or by_source.get("UN", 0),
            "MOHA List": by_source.get("MOHA_MALAYSIA", 0) or by_source.get("MOHA", 0),
            "PEP List": by_source.get("PEP_LIST", 0),
        }
        
        total = sum(filtered_source.values())
        
        return {
            "total_records": total,
            "by_source": filtered_source
        }
    except Exception as e:
        logger.error(f"Error getting official statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# XML Upload endpoint
@app.post("/api/upload/xml", response_model=UpdateResponse)
async def upload_xml(
    file: UploadFile = File(...),
    source: str = Form("XML_UPLOAD")
):
    """
    Upload XML file to add/update sanctions
    
    Parses the XML file and inserts records into the database.
    Handles duplicates automatically.
    """
    try:
        # Read file content
        content = await file.read()
        
        # Try to decode
        try:
            xml_content = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                xml_content = content.decode('latin-1')
            except UnicodeDecodeError:
                xml_content = content.decode('utf-8', errors='ignore')
        
        # Parse XML
        try:
            parser = get_parser(source)
            records = parser.parse(xml_content)
        except Exception as parse_err:
            logger.error(f"XML parsing error: {parse_err}")
            return UpdateResponse(
                success=False,
                message=f"Invalid XML format: {str(parse_err)}",
                records_added=0,
                records_skipped=0
            )
        
        if not records:
            return UpdateResponse(
                success=True,
                message="No records found in XML file",
                records_added=0,
                records_skipped=0
            )
        
        # Insert into database
        result = db.add_sanctions_batch(records, source=source.upper())
        
        # Log update
        db.log_update(
            source=source.upper(),
            update_type="XML_UPLOAD",
            added=result['added'],
            skipped=result['skipped'],
            status="SUCCESS"
        )
        
        return UpdateResponse(
            success=True,
            message=f"Successfully processed XML file",
            records_added=result['added'],
            records_skipped=result['skipped']
        )
        
    except Exception as e:
        logger.error(f"Error uploading XML: {e}")
        
        # Log failed update
        db.log_update(
            source=source.upper() if source else "XML_UPLOAD",
            update_type="XML_UPLOAD",
            status="FAILED",
            error_message=str(e)
        )
        
        raise HTTPException(status_code=500, detail=str(e))


# Automatic update endpoints
@app.post("/api/update/moha", response_model=UpdateResponse)
async def update_moha_list():
    """
    Fetch and update MOHA Malaysia sanctions list
    
    Downloads the latest list from MOHA website and updates the database.
    """
    try:
        url = config.SANCTIONS_URLS["MOHA_MALAYSIA"]
        
        # Fetch from URL
        records = fetch_and_parse(url, source_type='MOHA')
        
        if not records:
            return UpdateResponse(
                success=True,
                message="No records found in MOHA list (website may be unavailable)",
                records_added=0,
                records_skipped=0
            )
        
        # Insert into database
        result = db.add_sanctions_batch(records, source='MOHA_MALAYSIA')
        
        # Log update
        db.log_update(
            source="MOHA_MALAYSIA",
            update_type="AUTO_UPDATE",
            added=result['added'],
            skipped=result['skipped'],
            status="SUCCESS"
        )
        
        return UpdateResponse(
            success=True,
            message=f"Successfully updated MOHA list",
            records_added=result['added'],
            records_skipped=result['skipped']
        )
        
    except Exception as e:
        logger.error(f"Error updating MOHA list: {e}")
        
        db.log_update(
            source="MOHA_MALAYSIA",
            update_type="AUTO_UPDATE",
            status="FAILED",
            error_message=str(e)
        )
        
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update MOHA list: {str(e)}"
        )


@app.post("/api/update/un", response_model=UpdateResponse)
async def update_un_list():
    """
    Fetch and update UN sanctions list
    
    Downloads the latest UN consolidated list and updates the database.
    """
    try:
        url = config.SANCTIONS_URLS["UN_LIST"]
        
        # Fetch from URL
        records = fetch_and_parse(url, source_type='UN')
        
        if not records:
            return UpdateResponse(
                success=True,
                message="No records found in UN list (website may be unavailable)",
                records_added=0,
                records_skipped=0
            )
        
        # Insert into database
        result = db.add_sanctions_batch(records, source='UN')
        
        # Log update
        db.log_update(
            source="UN",
            update_type="AUTO_UPDATE",
            added=result['added'],
            skipped=result['skipped'],
            status="SUCCESS"
        )
        
        return UpdateResponse(
            success=True,
            message=f"Successfully updated UN list",
            records_added=result['added'],
            records_skipped=result['skipped']
        )
        
    except Exception as e:
        logger.error(f"Error updating UN list: {e}")
        
        db.log_update(
            source="UN",
            update_type="AUTO_UPDATE",
            status="FAILED",
            error_message=str(e)
        )
        
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update UN list: {str(e)}"
        )


@app.post("/api/update/all", response_model=Dict)
async def update_all_lists():
    """
    Update all sanctions lists (MOHA and UN)
    """
    results = {}
    
    # Update MOHA
    try:
        await update_moha_list()
        results["moha"] = "success"
    except Exception as e:
        results["moha"] = f"failed: {str(e)}"
    
    # Update UN
    try:
        await update_un_list()
        results["un"] = "success"
    except Exception as e:
        results["un"] = f"failed: {str(e)}"
    
    return {
        "success": True,
        "results": results
    }


# Refresh from local files endpoint
@app.post("/api/refresh/local", response_model=Dict)
async def refresh_from_local_files():
    """
    Refresh sanctions data from local XML files
    This allows reloading data after editing the XML files
    """
    results = {}
    
    # Refresh MOHA
    try:
        moha_file = config.LOCAL_XML_FILES.get("MOHA_MALAYSIA")
        if moha_file and moha_file.exists():
            records = parse_local_file(str(moha_file), source_type='MOHA')
            if records:
                result = db.add_sanctions_batch(records, source='MOHA')
                db.log_update(
                    source="MOHA",
                    update_type="LOCAL_REFRESH",
                    added=result['added'],
                    skipped=result['skipped'],
                    status="SUCCESS"
                )
                results["moha"] = {"added": result['added'], "skipped": result['skipped']}
            else:
                results["moha"] = "No records found"
        else:
            results["moha"] = "File not found"
    except Exception as e:
        logger.error(f"Error refreshing MOHA from local: {e}")
        results["moha"] = f"Error: {str(e)}"
    
    # Refresh UN
    try:
        un_file = config.LOCAL_XML_FILES.get("UN_LIST")
        if un_file and un_file.exists():
            records = parse_local_file(str(un_file), source_type='UN')
            if records:
                result = db.add_sanctions_batch(records, source='UN')
                db.log_update(
                    source="UN",
                    update_type="LOCAL_REFRESH",
                    added=result['added'],
                    skipped=result['skipped'],
                    status="SUCCESS"
                )
                results["un"] = {"added": result['added'], "skipped": result['skipped']}
            else:
                results["un"] = "No records found"
        else:
            results["un"] = "File not found"
    except Exception as e:
        logger.error(f"Error refreshing UN from local: {e}")
        results["un"] = f"Error: {str(e)}"
    
    # Refresh PEP
    try:
        pep_file = config.LOCAL_XML_FILES.get("PEP_LIST")
        if pep_file and pep_file.exists():
            records = parse_local_file(str(pep_file), source_type='PEP')
            if records:
                result = db.add_sanctions_batch(records, source='PEP_LIST')
                db.log_update(
                    source="PEP_LIST",
                    update_type="LOCAL_REFRESH",
                    added=result['added'],
                    skipped=result['skipped'],
                    status="SUCCESS"
                )
                results["pep"] = {"added": result['added'], "skipped": result['skipped']}
            else:
                results["pep"] = "No records found"
        else:
            results["pep"] = "File not found"
    except Exception as e:
        logger.error(f"Error refreshing PEP from local: {e}")
        results["pep"] = f"Error: {str(e)}"
    
    return {
        "success": True,
        "message": "Local files refreshed",
        "results": results
    }


@app.post("/api/refresh/local/{source_type}", response_model=UpdateResponse)
async def refresh_specific_source(source_type: str):
    """
    Refresh a specific source from local file (MOHA, UN, or PEP)
    """
    source = source_type.upper()
    
    if source not in ["MOHA", "UN", "PEP"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid source. Only MOHA, UN, and PEP are allowed"
        )
    
    try:
        if source == "MOHA":
            file_key = "MOHA_MALAYSIA"
        elif source == "PEP":
            file_key = "PEP_LIST"
        else:
            file_key = "UN_LIST"
        
        local_file = config.LOCAL_XML_FILES.get(file_key)
        if not local_file or not local_file.exists():
            return UpdateResponse(
                success=False,
                message=f"Local file not found for {source}",
                records_added=0,
                records_skipped=0
            )
        
        records = parse_local_file(str(local_file), source_type=source)
        
        if not records:
            return UpdateResponse(
                success=True,
                message=f"No records found in {source} file",
                records_added=0,
                records_skipped=0
            )
        
        result = db.add_sanctions_batch(records, source=source)
        
        db.log_update(
            source=source,
            update_type="LOCAL_REFRESH",
            added=result['added'],
            skipped=result['skipped'],
            status="SUCCESS"
        )
        
        return UpdateResponse(
            success=True,
            message=f"Successfully refreshed {source} from local file",
            records_added=result['added'],
            records_skipped=result['skipped']
        )
        
    except Exception as e:
        logger.error(f"Error refreshing {source} from local: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh {source}: {str(e)}"
        )


# Update history endpoint
@app.get("/api/history", response_model=List[Dict])
async def get_update_history(limit: int = Query(50, ge=1, le=200)):
    """Get update history"""
    try:
        history = db.get_update_history(limit=limit)
        return history
    except Exception as e:
        logger.error(f"Error getting history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Manual entry endpoint
# Only UN and MOHA are allowed as sources
ALLOWED_SOURCES = ["UN", "MOHA"]

class SanctionInput(BaseModel):
    full_name: str
    date_of_birth: Optional[str] = None
    nationality: Optional[str] = None
    id_number: Optional[str] = None
    id_type: Optional[str] = None
    source: str = "MOHA"  # Default to MOHA
    listing_date: Optional[str] = None
    comments: Optional[str] = None

@app.post("/api/sanctions", response_model=Dict)
async def add_sanction(request: SanctionInput):
    """Add a single sanction record manually"""
    # Validate source - only UN and MOHA allowed
    source = request.source.upper()
    if source not in ALLOWED_SOURCES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid source. Only {', '.join(ALLOWED_SOURCES)} are allowed"
        )
    
    try:
        result = db.add_sanction(
            name=request.full_name,
            dob=request.date_of_birth,
            nationality=request.nationality,
            id_number=request.id_number,
            id_type=request.id_type,
            source=source,
            listing_date=request.listing_date,
            comments=request.comments
        )
        
        if result:
            return {"success": True, "message": "Sanction added successfully"}
        else:
            return {"success": False, "message": "Duplicate record exists"}
            
    except Exception as e:
        logger.error(f"Error adding sanction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Delete sanction endpoint
@app.delete("/api/sanctions/{sanction_id}")
async def delete_sanction(sanction_id: int):
    """Delete (soft delete) a sanction record"""
    try:
        result = db.delete_sanction(sanction_id)
        if result:
            return {"success": True, "message": "Sanction deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Sanction not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting sanction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Settings endpoint
@app.get("/api/settings")
async def get_settings():
    """Get current system settings"""
    return {
        "FUZZY_MATCH_THRESHOLD": config.FUZZY_MATCH_THRESHOLD,
        "WEIGHTS": config.WEIGHTS,
        "RISK_LEVELS": config.RISK_LEVELS,
        "SANCTIONS_URLS": config.SANCTIONS_URLS,
        "AUTO_UPDATE_ENABLED": config.AUTO_UPDATE_ENABLED
    }


# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
