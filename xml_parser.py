"""
XML Parser for Sanctions Lists
Handles parsing of MOHA Malaysia, UN, and custom XML formats
"""

import xml.etree.ElementTree as ET
import logging
import re
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
import config

# Configure logging
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


class XMLParser:
    """Base class for XML sanctions list parsing"""
    
    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        # Remove extra whitespace
        text = ' '.join(text.strip().split())
        return text
    
    @staticmethod
    def _extract_date(text: str) -> Optional[str]:
        """Extract date from text in various formats"""
        if not text:
            return None
        
        text = text.strip()
        
        # Try ISO format
        if re.match(r'\d{4}-\d{2}-\d{2}', text):
            return text[:10]
        
        # Try common formats
        date_formats = [
            '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y',
            '%m/%d/%Y', '%m-%d-%Y',
            '%Y/%m/%d', '%Y-%m-%d',
            '%B %d, %Y', '%d %B %Y'
        ]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(text, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # Extract year only if that's all we can get
        year_match = re.search(r'\b(19|20)\d{2}\b', text)
        if year_match:
            return year_match.group()
        
        return text if text else None
    
    def parse(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse XML content - to be implemented by subclasses"""
        raise NotImplementedError


class MOHAParser(XMLParser):
    """
    Parser for MOHA Malaysia sanctions list
    Format: Based on KDN (Kementerian Dalam Negeri) list
    Supports both standard XML and the specific table-based MOHA format
    """
    
    def parse(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse MOHA Malaysia XML"""
        records = []
        
        try:
            # Clean XML content
            xml_content = xml_content.strip()
            if xml_content.startswith('<?xml'):
                end_decl = xml_content.find('?>')
                if end_decl != -1:
                    xml_content = xml_content[end_decl+2:]
            
            # First, try regex-based parsing (more robust for malformed XML)
            try:
                records = self._parse_with_regex(xml_content)
                if records:
                    logger.info(f"Parsed {len(records)} records from MOHA XML using regex")
                    return records
            except Exception as regex_err:
                logger.debug(f"Regex parsing failed, trying XML parser: {regex_err}")
            
            # Fallback to standard XML parsing
            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError as e:
                error_msg = str(e)
                # Try with different encoding
                try:
                    root = ET.fromstring(xml_content.encode('utf-8'))
                except ET.ParseError:
                    # Try to wrap content
                    xml_content = '<root>' + xml_content + '</root>'
                    root = ET.fromstring(xml_content)
            
            # First, try the table-based MOHA format with <entry> elements
            entries = root.findall('.//entry')
            
            if entries:
                # Table-based format
                for entry in entries:
                    record = self._parse_moha_entry(entry)
                    if record and record.get('name'):
                        records.append(record)
            else:
                # Try standard MOHA format
                possible_roots = ['INDIVIDUALS', 'INDIVIDUAL', 'LIST', 'CONSOLIDATEDLIST', 'SANCTIONSLIST', 'xmlResponse']
                
                for root_name in possible_roots:
                    entries = root.findall(f'.//{root_name}')
                    if entries:
                        break
                
                if not entries:
                    entries = list(root)
                
                for entry in entries:
                    record = self._parse_entry(entry)
                    if record and record.get('name'):
                        records.append(record)
            
            logger.info(f"Parsed {len(records)} records from MOHA XML")
            
        except Exception as e:
            logger.error(f"Error parsing MOHA XML: {e}")
            raise
        
        return records
    
    def _parse_moha_entry(self, entry: ET.Element) -> Dict[str, Any]:
        """Parse entry in MOHA table-based format with <field name=...> elements"""
        record = {
            'name': None,
            'dob': None,
            'nationality': 'MALAYSIA',
            'id_number': None,
            'id_type': None,
            'source': 'MOHA_MALAYSIA',
            'listing_date': None,
            'comments': None
        }
        
        # Get all field elements
        fields = entry.findall('.//field')
        
        for field in fields:
            field_name = field.get('name', '')
            field_text = field.text
            
            if not field_text:
                continue
            
            # Map MOHA field names to our record fields
            # Name field
            if field_name in ['(3) Name', 'Name', 'NAME', '3']:
                if not record['name']:
                    record['name'] = self._clean_text(field_text)
            
            # DOB field
            elif field_name in ['(6) Date of Birth', 'Date of Birth', 'DOB', '6']:
                if not record['dob']:
                    record['dob'] = self._extract_date(field_text)
            
            # Nationality field
            elif field_name in ['(9) Nationality', 'Nationality', 'NATIONALITY', '9']:
                record['nationality'] = self._clean_text(field_text).upper()
            
            # Passport field
            elif field_name in ['(10) Passport Number', 'Passport Number', 'PASSPORT', '10']:
                if not record['id_number']:
                    record['id_number'] = self._clean_text(field_text)
                    record['id_type'] = 'PASSPORT'
            
            # ID Card field
            elif field_name in ['(11) Identification Card Number', 'IC Number', 'ID', '11']:
                if not record['id_number']:
                    record['id_number'] = self._clean_text(field_text)
                    record['id_type'] = 'IC'
            
            # Listing date
            elif field_name in ['(13) Date 0f Listed', 'Date Listed', 'Date of Listed', '13']:
                record['listing_date'] = self._extract_date(field_text)
            
            # Address/Designation as comments
            elif field_name in ['(5) Designation', 'Designation', '(12) Address', 'Address']:
                if field_text and field_text != '-':
                    record['comments'] = self._clean_text(field_text)
        
        return record
    
    def _parse_with_regex(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse MOHA XML using regex for malformed XML"""
        records = []
        
        try:
            # Find all entry blocks using regex
            entry_pattern = re.compile(r'<entry[^>]*>(.*?)</entry>', re.DOTALL | re.IGNORECASE)
            field_pattern = re.compile(r'<field\s+name="([^"]+)"[^>]*>([^<]*)</field>', re.IGNORECASE)
            
            entry_matches = entry_pattern.findall(xml_content)
            
            for entry_content in entry_matches:
                record = {
                    'name': None,
                    'dob': None,
                    'nationality': 'MALAYSIA',
                    'id_number': None,
                    'id_type': None,
                    'source': 'MOHA_MALAYSIA',
                    'listing_date': None,
                    'comments': None
                }
                
                # Find all fields in this entry
                fields = field_pattern.findall(entry_content)
                
                for field_name, field_value in fields:
                    field_value = field_value.strip()
                    if not field_value or field_value == '-':
                        continue
                    
                    # Map fields
                    if field_name in ['(3) Name', 'Name', 'NAME', '3']:
                        if not record['name']:
                            record['name'] = self._clean_text(field_value)
                    elif field_name in ['(6) Date of Birth', 'Date of Birth', 'DOB', '6']:
                        if not record['dob']:
                            record['dob'] = self._extract_date(field_value)
                    elif field_name in ['(9) Nationality', 'Nationality', 'NATIONALITY', '9']:
                        record['nationality'] = self._clean_text(field_value).upper()
                    elif field_name in ['(10) Passport Number', 'Passport Number', 'PASSPORT', '10']:
                        if not record['id_number']:
                            record['id_number'] = self._clean_text(field_value)
                            record['id_type'] = 'PASSPORT'
                    elif field_name in ['(11) Identification Card Number', 'IC Number', 'ID', '11']:
                        if not record['id_number']:
                            record['id_number'] = self._clean_text(field_value)
                            record['id_type'] = 'IC'
                    elif field_name in ['(13) Date 0f Listed', 'Date Listed', 'Date of Listed', '13']:
                        record['listing_date'] = self._extract_date(field_value)
                    elif field_name in ['(5) Designation', 'Designation', '(12) Address', 'Address']:
                        if field_value and field_value != '-':
                            record['comments'] = self._clean_text(field_value)
                
                if record.get('name'):
                    records.append(record)
            
        except Exception as e:
            logger.error(f"Error in regex parsing: {e}")
        
        return records
    
    def _parse_entry(self, entry: ET.Element) -> Dict[str, Any]:
        """Parse a single entry from MOHA XML"""
        record = {
            'name': None,
            'dob': None,
            'nationality': 'MALAYSIA',  # MOHA list is Malaysia-specific
            'id_number': None,
            'id_type': None,
            'source': 'MOHA_MALAYSIA',
            'listing_date': None,
            'comments': None
        }
        
        # Try different possible tag names for name
        name_tags = ['NAME', 'FULL_NAME', 'INDIVIDUAL_NAME', 'FIRST_NAME', 'TITLE', 'FULL_NAME_EN']
        for tag in name_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                record['name'] = self._clean_text(element.text)
                break
        
        # If still no name found, try getting text from entry directly
        if not record['name']:
            if entry.text:
                record['name'] = self._clean_text(entry.text)
        
        # Try to get DOB
        dob_tags = ['DOB', 'DATE_OF_BIRTH', 'BIRTH_DATE', 'BIRTHDAY', 'BIRTH_DATE_DAY', 'DATE_OF_BIRTH_YEAR']
        for tag in dob_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                record['dob'] = self._extract_date(element.text)
                break
        
        # Try to get nationality
        nat_tags = ['NATIONALITY', 'CITIZENSHIP', 'COUNTRY', 'NATIONALITY_COUNTRY']
        for tag in nat_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                record['nationality'] = self._clean_text(element.text).upper()
                break
        
        # Try to get ID/Passport number
        id_tags = ['ID_NUMBER', 'PASSPORT', 'PASSPORT_NUMBER', 'ID', 'NATIONAL_ID', 'DOCUMENT_NUMBER']
        for tag in id_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                record['id_number'] = self._clean_text(element.text)
                record['id_type'] = 'PASSPORT' if 'PASS' in tag else 'ID'
                break
        
        # Try to get listing date
        date_tags = ['LISTING_DATE', 'DATE_LISTED', 'DATE_ADDED', 'CREATED_DATE']
        for tag in date_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                record['listing_date'] = self._extract_date(element.text)
                break
        
        # Get any comments/notes
        comment_tags = ['COMMENT', 'COMMENTS', 'NOTE', 'NOTES', 'REMARK', 'REMARKS']
        comments = []
        for tag in comment_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                comments.append(self._clean_text(element.text))
        if comments:
            record['comments'] = ' | '.join(comments)
        
        return record


class UNParser(XMLParser):
    """
    Parser for UN Sanctions List
    Format: Standard UN consolidated list XML
    Supports both standard UN format and various UN list variations
    """
    
    def parse(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse UN sanctions list XML"""
        records = []
        
        try:
            # Clean XML content
            xml_content = xml_content.strip()
            if xml_content.startswith('<?xml'):
                end_decl = xml_content.find('?>')
                if end_decl != -1:
                    xml_content = xml_content[end_decl+2:]
            
            # Try to parse, handle errors gracefully
            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError:
                try:
                    root = ET.fromstring(xml_content.encode('utf-8'))
                except ET.ParseError:
                    xml_content = '<root>' + xml_content + '</root>'
                    root = ET.fromstring(xml_content)
            
            # UN list uses <INDIVIDUAL> and <ENTITY> tags
            individuals = root.findall('.//INDIVIDUAL') + root.findall('.//Individual')
            entities = root.findall('.//ENTITY') + root.findall('.//Entity')
            
            # Parse individuals
            for individual in individuals:
                record = self._parse_individual(individual)
                if record:
                    records.append(record)
            
            # Parse entities (organizations)
            for entity in entities:
                record = self._parse_entity(entity)
                if record:
                    records.append(record)
            
            logger.info(f"Parsed {len(records)} records from UN XML")
            
        except Exception as e:
            logger.error(f"Error parsing UN XML: {e}")
            raise
        
        return records
    
    def _parse_individual(self, element: ET.Element) -> Optional[Dict[str, Any]]:
        """Parse an individual entry"""
        record = {
            'name': None,
            'dob': None,
            'nationality': None,
            'id_number': None,
            'id_type': None,
            'source': 'UN',
            'listing_date': None,
            'comments': None
        }
        
        # Get name from various name elements
        # UN list can have: FIRST_NAME, SECOND_NAME, THIRD_NAME, LAST_NAME
        name_elements = (
            element.findall('.//FIRST_NAME') + 
            element.findall('.//FIRSTNAME') +
            element.findall('.//SECOND_NAME') + 
            element.findall('.//SECONDNAME') +
            element.findall('.//THIRD_NAME') + 
            element.findall('.//THIRDNAME')
        )
        last_name_elements = element.findall('.//LAST_NAME') + element.findall('.//LASTNAME')
        
        name_parts = []
        for el in name_elements:
            if el.text:
                name_parts.append(self._clean_text(el.text))
        
        for el in last_name_elements:
            if el.text:
                name_parts.append(self._clean_text(el.text))
        
        if name_parts:
            record['name'] = ' '.join(name_parts)
        
        # If no structured name, try TITLE
        if not record['name']:
            title_elements = element.findall('.//TITLE')
            for el in title_elements:
                if el.text:
                    record['name'] = self._clean_text(el.text)
                    break
        
        # Get DOB
        dob_elements = element.findall('.//DATE_OF_BIRTH') + element.findall('.//DOB')
        for el in dob_elements:
            if el.text:
                record['dob'] = self._extract_date(el.text)
                break
        
        # Get nationality
        nat_elements = element.findall('.//NATIONALITY') + element.findall('.//NATIONALITY/VALUE')
        for el in nat_elements:
            if el.text:
                record['nationality'] = self._clean_text(el.text).upper()
                break
        
        # Get ID/Passport
        doc_elements = element.findall('.//DOCUMENT') + element.findall('.//IDENTITY_DOCUMENT')
        for doc in doc_elements:
            type_el = doc.find('.//TYPE_OF_DOCUMENT') or doc.find('.//DOCUMENT_TYPE')
            number_el = doc.find('.//NUMBER') or doc.find('.//DOCUMENT_NUMBER')
            
            if number_el is not None and number_el.text:
                record['id_number'] = self._clean_text(number_el.text)
                if type_el is not None and type_el.text:
                    record['id_type'] = self._clean_text(type_el.text).upper()
                break
        
        # Get listing date
        date_elements = element.findall('.//LISTING_DATE') + element.findall('.//DATE_LISTED')
        for el in date_elements:
            if el.text:
                record['listing_date'] = self._extract_date(el.text)
                break
        
        # Get comments
        comment_elements = element.findall('.//COMMENTS') + element.findall('.//NOTE')
        comments = []
        for el in comment_elements:
            if el.text:
                comments.append(self._clean_text(el.text))
        if comments:
            record['comments'] = ' | '.join(comments)
        
        return record if record['name'] else None
    
    def _parse_entity(self, element: ET.Element) -> Optional[Dict[str, Any]]:
        """Parse an entity (organization) entry"""
        record = {
            'name': None,
            'dob': None,
            'nationality': None,
            'id_number': None,
            'id_type': None,
            'source': 'UN',
            'listing_date': None,
            'comments': None
        }
        
        # Get entity name
        name_elements = element.findall('.//NAME') + element.findall('.//ENTITY_NAME')
        for el in name_elements:
            if el.text:
                record['name'] = self._clean_text(el.text)
                break
        
        # Get listing date
        date_elements = element.findall('.//LISTING_DATE') + element.findall('.//DATE_LISTED')
        for el in date_elements:
            if el.text:
                record['listing_date'] = self._extract_date(el.text)
                break
        
        # Get comments
        comment_elements = element.findall('.//COMMENTS') + element.findall('.//NOTE')
        comments = []
        for el in comment_elements:
            if el.text:
                comments.append(self._clean_text(el.text))
        if comments:
            record['comments'] = ' | '.join(comments)
        
        return record if record['name'] else None


class GenericXMLParser(XMLParser):
    """
    Generic parser for custom XML formats
    Attempts to find common field patterns
    """
    
    def parse(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse generic XML"""
        records = []
        
        try:
            # Clean XML content - remove BOM, extra whitespace, and XML declarations
            xml_content = xml_content.strip()
            # Remove XML declaration if present
            if xml_content.startswith('<?xml'):
                end_decl = xml_content.find('?>')
                if end_decl != -1:
                    xml_content = xml_content[end_decl+2:]
            
            # Check if we have multiple root elements or junk after root
            # Wrap in root element if needed
            if not xml_content.startswith('<'):
                xml_content = '<root>' + xml_content + '</root>'
            elif xml_content.count('<') > 1:
                # More than one tag - might have multiple roots or junk
                # Find the first and last tags to wrap them
                first_tag_start = xml_content.find('<')
                last_tag_end = xml_content.rfind('>') + 1
                if last_tag_end > first_tag_start:
                    cleaned = xml_content[first_tag_start:last_tag_end]
                    # If still doesn't start with a proper root, wrap it
                    if not cleaned.startswith('<'):
                        xml_content = '<root>' + cleaned + '</root>'
                    else:
                        xml_content = cleaned
            
            # Try to parse, if fails, wrap everything in a root
            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError:
                # Last resort: wrap the entire content
                xml_content = '<root>' + xml_content + '</root>'
                root = ET.fromstring(xml_content)
            
            # Find all record elements (try common names)
            possible_record_tags = [
                'RECORD', 'RECORDS', 'ENTRY', 'PERSON', 'INDIVIDUAL',
                'SANCTION', 'SANCTIONS', 'ITEM', 'ROW', 'DATA'
            ]
            
            entries = []
            for tag in possible_record_tags:
                entries = root.findall(f'.//{tag}')
                if entries:
                    break
            
            # If still no entries, try direct children
            if not entries:
                entries = list(root)
            
            for entry in entries:
                record = self._parse_generic_entry(entry)
                if record and record.get('name'):
                    records.append(record)
            
            logger.info(f"Parsed {len(records)} records from generic XML")
            
        except Exception as e:
            logger.error(f"Error parsing generic XML: {e}")
            raise
        
        return records
    
    def _parse_generic_entry(self, entry: ET.Element) -> Dict[str, Any]:
        """Parse a generic entry"""
        record = {
            'name': None,
            'dob': None,
            'nationality': None,
            'id_number': None,
            'id_type': None,
            'source': 'XML_UPLOAD',
            'listing_date': None,
            'comments': None
        }
        
        # Look for name (case-insensitive)
        name_tags = ['name', 'fullname', 'full_name', 'individual', 'person', 'title', 
                     'NAME', 'FULL_NAME', 'FULLNAME', 'INDIVIDUAL']
        for tag in name_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                record['name'] = self._clean_text(element.text)
                break
        
        # Look for DOB
        dob_tags = ['dob', 'dateofbirth', 'date_of_birth', 'birthdate', 'birth_date', 'birthday',
                    'DOB', 'DATE_OF_BIRTH', 'BIRTH_DATE']
        for tag in dob_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                record['dob'] = self._extract_date(element.text)
                break
        
        # Look for nationality
        nat_tags = ['nationality', 'citizenship', 'country', 'nationality_country',
                    'NATIONALITY', 'CITIZENSHIP', 'COUNTRY']
        for tag in nat_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                record['nationality'] = self._clean_text(element.text).upper()
                break
        
        # Look for ID
        id_tags = ['id', 'idnumber', 'id_number', 'passport', 'passportnumber', 'document', 'docnumber']
        for tag in id_tags:
            element = entry.find(f'.//{tag}')
            if element is not None and element.text:
                record['id_number'] = self._clean_text(element.text)
                record['id_type'] = 'PASSPORT' if 'passport' in tag.lower() else 'ID'
                break
        
        return record


def get_parser(source_type: str) -> XMLParser:
    """Get appropriate parser for source type"""
    parsers = {
        'MOHA': MOHAParser(),
        'UN': UNParser(),
        'GENERIC': GenericXMLParser(),
    }
    return parsers.get(source_type.upper(), GenericXMLParser())


def fetch_and_parse(url: str, source_type: str = 'GENERIC', 
                   timeout: int = None) -> List[Dict[str, Any]]:
    """
    Fetch XML from URL and parse it
    
    Args:
        url: URL to fetch
        source_type: Type of source (MOHA, UN, GENERIC)
        timeout: Request timeout in seconds
    
    Returns:
        List of parsed records
    """
    timeout = timeout or config.REQUEST_TIMEOUT
    
    logger.info(f"Fetching XML from {url}")
    
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        
        xml_content = response.text
        
        parser = get_parser(source_type)
        records = parser.parse(xml_content)
        
        logger.info(f"Successfully fetched and parsed {len(records)} records")
        return records
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching XML: {e}")
        raise
    except Exception as e:
        logger.error(f"Error parsing XML: {e}")
        raise


def parse_local_file(file_path: str, source_type: str = 'GENERIC') -> List[Dict[str, Any]]:
    """
    Parse XML from local file
    
    Args:
        file_path: Path to local XML file
        source_type: Type of source (MOHA, UN, GENERIC)
    
    Returns:
        List of parsed records
    """
    logger.info(f"Parsing local XML file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        parser = get_parser(source_type)
        records = parser.parse(xml_content)
        
        logger.info(f"Successfully parsed {len(records)} records from local file")
        return records
        
    except FileNotFoundError:
        logger.error(f"Local file not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error parsing local XML: {e}")
        raise
