"""
Fuzzy Matching Engine for Sanctions Screening
Uses rapidfuzz for efficient fuzzy string matching
"""

import logging
import re
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    # Fallback to difflib
    def fuzz_ratio(s1: str, s2: str) -> float:
        return SequenceMatcher(None, s1.lower(), s2.lower()).ratio() * 100
    def fuzz_partial_ratio(s1: str, s2: str) -> float:
        return fuzz_ratio(s1, s2)  # Simplified fallback
    def fuzz_token_sort_ratio(s1: str, s2: str) -> float:
        s1_sorted = ' '.join(sorted(s1.lower().split()))
        s2_sorted = ' '.join(sorted(s2.lower().split()))
        return fuzz_ratio(s1_sorted, s2_sorted)
    class fuzz:
        ratio = staticmethod(fuzz_ratio)
        partial_ratio = staticmethod(fuzz_partial_ratio)
        token_sort_ratio = staticmethod(fuzz_token_sort_ratio)

import config

# Configure logging
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Represents a single match result"""
    sanction_id: int
    full_name: str
    match_score: float
    name_score: float
    dob_match: bool
    dob_score: float
    nationality_match: bool
    id_match: bool
    id_score: float
    risk_level: str
    source: str
    matched_fields: List[str]
    is_exact_match: bool = False  # 100% match flag
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "sanction_id": self.sanction_id,
            "full_name": self.full_name,
            "match_score": round(self.match_score, 2),
            "name_score": round(self.name_score, 2),
            "dob_match": self.dob_match,
            "nationality_match": self.nationality_match,
            "id_match": self.id_match,
            "risk_level": self.risk_level,
            "source": self.source,
            "matched_fields": self.matched_fields,
            "is_exact_match": self.is_exact_match
        }

    def check_exact_match(self, query_dob: str = None, query_nationality: str = None, 
                          query_id: str = None, target_dob: str = None, 
                          target_nationality: str = None, target_id: str = None) -> bool:
        """
        Check if this is a 100% exact match based on the criteria:
        1. All 4 fields match (name, DOB, nationality, ID) - name_score must be >= 95
        2. DOB matches AND (no ID in database OR no ID provided) - name_score >= 95
        3. DOB AND nationality match - name_score >= 95
        
        Avoid 100% when only name and nationality match (need more factors like DOB)
        """
        # First, name must be very similar (>=95%)
        if self.name_score < 95:
            return False
        
        # Condition 1: All 4 fields match
        if (self.dob_match and self.nationality_match and self.id_match and target_id):
            return True
        
        # Condition 2: DOB matches + no ID in database OR no ID provided
        has_db_id = bool(target_id and target_id.strip())
        has_query_id = bool(query_id and query_id.strip())
        
        if self.dob_match:
            # DOB matches, check if no ID conflict
            if not has_db_id and not has_query_id:
                # No ID in DB and no ID provided - potential match
                return True
            elif not has_db_id and has_query_id:
                # DB has no ID but query has ID - can't confirm, but DOB matches
                return True
            elif has_db_id and not has_query_id:
                # DB has ID but query has no ID - DOB matches is strong signal
                return True
        
        # Condition 3: DOB AND nationality match (both must match)
        if self.dob_match and self.nationality_match:
            return True
        
        return False


class FuzzyMatchingEngine:
    """Fuzzy matching engine for sanctions screening"""
    
    def __init__(self, threshold: int = None):
        """Initialize the fuzzy matching engine"""
        self.threshold = threshold or config.FUZZY_MATCH_THRESHOLD
        self.weights = config.WEIGHTS
        self.risk_levels = config.RISK_LEVELS
        
        logger.info(f"Fuzzy matching engine initialized with threshold: {self.threshold}%")
    
    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize name for comparison"""
        if not name:
            return ""
        
        # Convert to uppercase
        name = name.upper().strip()
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Remove common prefixes/suffixes
        prefixes_to_remove = ['DATO', 'DATIN', 'TAN SRI', 'TUN', 'ENCIK', 'PUAN', 'MR', 'MRS', 'MS']
        for prefix in prefixes_to_remove:
            name = re.sub(rf'\b{prefix}\b\.?', '', name)
        
        return name.strip()
    
    @staticmethod
    def _extract_name_tokens(name: str) -> List[str]:
        """Extract name tokens (first name, middle names, last name)"""
        normalized = FuzzyMatchingEngine._normalize_name(name)
        return normalized.split()
    
    @staticmethod
    def _parse_dob(dob: str) -> Optional[str]:
        """Parse and normalize date of birth"""
        if not dob:
            return None
        
        # Try various date formats
        dob = str(dob).strip()
        
        # Extract year (assume 4-digit year or 2-digit with context)
        year_match = re.search(r'\b(19|20)\d{2}\b', dob)
        if year_match:
            return year_match.group()
        
        # Try to extract any 4-digit number that could be year
        numbers = re.findall(r'\d+', dob)
        for num in numbers:
            if 1900 <= int(num) <= 2010:
                return num
        
        return None
    
    def _calculate_name_score(self, query_name: str, target_name: str) -> Tuple[float, str]:
        """
        Calculate name similarity score
        Returns (score, match_type)
        
        IMPORTANT: Only 100% when full name is EXACTLY the same (full word match).
        Partial matches like "Muhammad Hafiz" vs "Muhammad Hafizzudin" should NOT be 100%.
        """
        query = self._normalize_name(query_name)
        target = self._normalize_name(target_name)
        
        if not query or not target:
            return 0.0, "none"
        
        # Exact match - only 100% when full names are identical
        if query == target:
            return 100.0, "exact"
        
        # Use more conservative scoring
        # Only use standard ratio and token_sort_ratio for stricter matching
        scores = []
        
        # Standard ratio - base similarity
        standard_score = fuzz.ratio(query, target)
        scores.append(standard_score)
        
        # Token sort ratio - for reordered names but with penalty
        token_score = fuzz.token_sort_ratio(query, target)
        scores.append(token_score)
        
        # Get best score but apply a penalty for non-exact matches
        best_score = max(scores)
        
        # If not exact match, reduce the score significantly
        # "Muhammad Hafiz" vs "Muhammad Hafizzudin" should not get high score
        if best_score < 100:
            # Apply a penalty - reduce by 20% for fuzzy matches
            best_score = best_score * 0.8
        
        # Determine match type (more conservative thresholds)
        if best_score >= 95:
            match_type = "exact"
        elif best_score >= 85:
            match_type = "very_high"
        elif best_score >= 70:
            match_type = "high"
        elif best_score >= 50:
            match_type = "medium"
        else:
            match_type = "low"
        
        return best_score, match_type
    
    def _calculate_dob_score(self, query_dob: str, target_dob: str) -> Tuple[bool, float]:
        """Calculate DOB match score"""
        query_year = self._parse_dob(query_dob)
        target_year = self._parse_dob(target_dob)
        
        if not query_year or not target_year:
            return False, 0.0
        
        # Exact year match
        if query_year == target_year:
            return True, 100.0
        
        # Partial match (off by 1 year - could be data entry error)
        try:
            diff = abs(int(query_year) - int(target_year))
            if diff <= 1:
                return True, 80.0
            elif diff <= 5:
                return True, 50.0
        except (ValueError, TypeError):
            pass
        
        return False, 0.0
    
    def _calculate_nationality_match(self, query_nationality: str, target_nationality: str) -> bool:
        """Check if nationalities match"""
        if not query_nationality or not target_nationality:
            return False
        
        q = query_nationality.upper().strip()
        t = target_nationality.upper().strip()
        
        # Exact match
        if q == t:
            return True
        
        # Common variations
        nationality_map = {
            'MALAYSIA': ['MYS', 'MALAYSIAN', 'MSIAN'],
            'SINGAPORE': ['SGP', 'SINGAPOREAN', 'SING'],
            'INDONESIA': ['IDN', 'INDONESIAN', 'IND'],
            'THAILAND': ['THA', 'THAI', 'TH'],
            'USA': ['US', 'UNITED STATES', 'AMERICAN', 'USA', 'US citizen'],
            'UK': ['UNITED KINGDOM', 'BRITISH', 'UK', 'GB', 'GREAT BRITAIN'],
            'IRAN': ['IRANIAN', 'IRAN', 'IR'],
            'NORTH KOREA': ['DPRK', 'NORTH KOREAN', 'NK'],
            'RUSSIAN': ['RUSSIA', 'RUSSIAN', 'RU', 'RUS'],
            'CHINESE': ['CHINA', 'CHINESE', 'CN', 'PRC'],
        }
        
        for standard, variations in nationality_map.items():
            if q in [standard] + variations or t in [standard] + variations:
                if q in [standard] + variations and t in [standard] + variations:
                    return True
        
        return False
    
    def _calculate_id_match(self, query_id: str, target_id: str) -> float:
        """Calculate ID number match score"""
        if not query_id or not target_id:
            return 0.0
        
        q = query_id.upper().strip()
        t = target_id.upper().strip()
        
        if q == t:
            return 100.0
        
        # Check for partial match (ID might contain extra characters)
        if q in t or t in q:
            return 80.0
        
        # Fuzzy match
        score = fuzz.ratio(q, t)
        return score if score >= 80 else 0.0
    
    def _determine_risk_level(self, match_score: float) -> str:
        """Determine risk level based on match score"""
        if match_score >= self.risk_levels["high"]:
            return "HIGH"
        elif match_score >= self.risk_levels["medium"]:
            return "MEDIUM"
        else:
            return "LOW"
    
    def screen_customer(self, full_name: str, dob: str = None, 
                       nationality: str = None, id_number: str = None,
                       include_below_threshold: bool = False) -> List[MatchResult]:
        """
        Screen a customer against the sanctions database
        
        Args:
            full_name: Customer's full name
            dob: Date of birth
            nationality: Nationality
            id_number: ID/Passport number
            include_below_threshold: Include matches below threshold
        
        Returns:
            List of MatchResult objects sorted by score (highest first)
        """
        from database import get_database
        
        db = get_database()
        
        # Get all sanctions for screening
        all_sanctions = db.get_all_sanctions(limit=10000)
        
        results = []
        
        for sanction in all_sanctions:
            matched_fields = []
            scores = []
            
            # Name matching (primary factor)
            name_score, name_match_type = self._calculate_name_score(
                full_name, sanction['full_name']
            )
            
            if name_score >= self.threshold:
                matched_fields.append("name")
                scores.append(name_score * (self.weights["name_exact"] / 100))
            else:
                continue  # Skip if name doesn't meet threshold
            
            # DOB matching (boost)
            dob_match, dob_score = self._calculate_dob_score(
                dob or "", sanction.get('date_of_birth', '')
            )
            if dob_match:
                matched_fields.append("dob")
                scores.append(dob_score * (self.weights["dob_match"] / 100))
            
            # Nationality matching (boost)
            nationality_match = self._calculate_nationality_match(
                nationality or "", sanction.get('nationality', '')
            )
            if nationality_match:
                matched_fields.append("nationality")
                scores.append(self.weights["nationality_match"])
            
            # ID matching (boost)
            target_id = sanction.get('id_number', '')
            id_score = self._calculate_id_match(
                id_number or "", target_id
            )
            if id_score > 0:
                matched_fields.append("id")
                scores.append(id_score * (self.weights["id_match"] / 100))
            
            # Calculate final match score
            # Check what user provided
            user_provided_dob = bool(dob and dob.strip())
            user_provided_id = bool(id_number and id_number.strip())
            user_provided_nationality = bool(nationality and nationality.strip())
            
            # Determine what matches AND user provided
            # Only count as additional factor if user provided the field AND it matches
            has_dob_match = dob_match and user_provided_dob
            has_id_match = (id_score > 0 and target_id) and user_provided_id
            has_nationality_match = nationality_match and user_provided_nationality
            
            # STRICT: Need DOB OR ID match for high score
            # If no DOB or ID provided by user, reduce score significantly regardless of other matches
            # Both DOB and ID are critical identifiers
            has_critical_match = has_dob_match or has_id_match
            
            # Additional factors count if DOB matches OR ID matches
            # Without DOB or ID, score should be reduced
            has_additional_factors = has_critical_match
            
            if scores:
                if has_additional_factors:
                    # Conservative scoring with DOB or ID
                    # Name + DOB = 50% (MEDIUM risk)
                    # Name + DOB + ID = higher (closer to 70%+)
                    if has_dob_match and not has_id_match:
                        # Name + DOB only = 50%
                        final_score = 50.0
                    elif has_id_match and not has_dob_match:
                        # Name + ID only = 50%
                        final_score = 50.0
                    elif has_dob_match and has_id_match:
                        # Name + DOB + ID = 70%
                        final_score = 70.0
                    else:
                        # Other combinations
                        final_score = (name_score * 0.7) + (sum(scores) * 0.3)
                        final_score = min(100.0, final_score)
                else:
                    # Only name matches - reduce score significantly to below 50%
                    # Use 40% of name score as the final score
                    final_score = name_score * 0.4
            else:
                # Only name score available
                final_score = name_score * 0.4
            
            # Determine if should include in results
            # Always include name matches (even if below threshold) but with reduced score
            # This is important for screening purposes
            
            if final_score >= self.threshold:
                risk_level = self._determine_risk_level(final_score)
            elif name_score >= self.threshold and not has_additional_factors:
                # Name matches but no additional factors - include as LOW risk
                # Use the reduced score (below 50%)
                final_score = name_score * 0.4
                risk_level = "LOW"
            elif include_below_threshold:
                risk_level = self._determine_risk_level(final_score)
            else:
                continue  # Skip results below threshold
            
            # Create result (only reaches here if not skipped)
            result = MatchResult(
                sanction_id=sanction['id'],
                full_name=sanction['full_name'],
                match_score=final_score,
                name_score=name_score,
                dob_match=dob_match,
                dob_score=dob_score,
                nationality_match=nationality_match,
                id_match=id_score > 0,
                id_score=id_score,
                risk_level=risk_level,
                source=sanction['source'],
                matched_fields=matched_fields
            )
            
            # Check for 100% exact match
            result.is_exact_match = result.check_exact_match(
                query_dob=dob,
                query_nationality=nationality,
                query_id=id_number,
                target_dob=sanction.get('date_of_birth', ''),
                target_nationality=sanction.get('nationality', ''),
                target_id=target_id
            )
            
            results.append(result)
        
        # Sort by match score (highest first)
        results.sort(key=lambda x: x.match_score, reverse=True)
        
        logger.info(f"Screening '{full_name}': {len(results)} potential matches found")
        
        return results
    
    def screen_name_only(self, name: str, limit: int = 10) -> List[Dict]:
        """
        Quick screening by name only (for autocomplete/typeahead)
        
        Args:
            name: Name to search
            limit: Maximum results
        
        Returns:
            List of matching sanctions with scores
        """
        from database import get_database
        
        db = get_database()
        normalized_name = self._normalize_name(name)
        
        # Get all sanctions
        all_sanctions = db.get_all_sanctions(limit=10000)
        
        results = []
        
        for sanction in all_sanctions:
            name_score, _ = self._calculate_name_score(
                normalized_name, sanction['full_name']
            )
            
            if name_score >= self.threshold:
                results.append({
                    'sanction_id': sanction['id'],
                    'full_name': sanction['full_name'],
                    'score': round(name_score, 2),
                    'source': sanction['source'],
                    'nationality': sanction.get('nationality')
                })
        
        # Sort and limit
        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:limit]


# Singleton instance
_matching_engine = None

def get_matching_engine() -> FuzzyMatchingEngine:
    """Get matching engine singleton instance"""
    global _matching_engine
    if _matching_engine is None:
        _matching_engine = FuzzyMatchingEngine()
    return _matching_engine
