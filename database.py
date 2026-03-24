"""
Sanctions Database Module
SQLite-based storage for sanctions list data
"""

import sqlite3
import logging
import hashlib
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path
import config

# Configure logging
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


class SanctionsDatabase:
    """SQLite database manager for sanctions list"""
    
    def __init__(self, db_path: str = None):
        """Initialize database connection"""
        self.db_path = db_path or str(config.DATABASE_PATH)
        self._init_database()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_database(self):
        """Initialize database schema"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Create sanctions table
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
        
        # Create indexes for faster searching
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_name ON sanctions(full_name)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_nationality ON sanctions(nationality)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_source ON sanctions(source)
        ''')
        
        # Create update history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS update_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                update_type TEXT NOT NULL,
                records_added INTEGER DEFAULT 0,
                records_updated INTEGER DEFAULT 0,
                records_skipped INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                error_message TEXT,
                executed_at TEXT NOT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    @staticmethod
    def _normalize_text(text: str) -> str:
        """Normalize and clean text data"""
        if not text:
            return None
        
        # Convert to uppercase and strip whitespace
        text = text.strip().upper()
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text if text else None
    
    @staticmethod
    def _generate_hash(name: str, dob: str, id_number: str) -> str:
        """Generate unique hash for deduplication"""
        data = f"{name}|{dob}|{id_number}".upper()
        return hashlib.md5(data.encode()).hexdigest()
    
    def add_sanction(self, name: str, dob: str = None, nationality: str = None,
                    id_number: str = None, id_type: str = None, source: str = "UNKNOWN",
                    listing_date: str = None, comments: str = None) -> bool:
        """Add a new sanction record"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Normalize data
            name = self._normalize_text(name)
            dob = self._normalize_text(dob)
            nationality = self._normalize_text(nationality)
            id_number = self._normalize_text(id_number)
            source = self._normalize_text(source)
            
            # Generate unique hash
            unique_hash = self._generate_hash(name, dob, id_number)
            
            now = datetime.now().isoformat()
            
            # Insert or ignore (avoid duplicates)
            cursor.execute('''
                INSERT OR IGNORE INTO sanctions 
                (unique_hash, full_name, date_of_birth, nationality, id_number, 
                 id_type, source, listing_date, comments, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (unique_hash, name, dob, nationality, id_number, id_type, 
                  source, listing_date, comments, now, now))
            
            result = cursor.rowcount > 0
            conn.commit()
            
            if result:
                logger.info(f"Added sanction: {name}")
            else:
                logger.debug(f"Duplicate skipped: {name}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error adding sanction: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def add_sanctions_batch(self, records: List[Dict[str, Any]], source: str = "BATCH") -> Dict[str, int]:
        """Add multiple sanction records efficiently"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        added = 0
        skipped = 0
        
        try:
            now = datetime.now().isoformat()
            
            for record in records:
                name = self._normalize_text(record.get('name'))
                dob = self._normalize_text(record.get('dob'))
                nationality = self._normalize_text(record.get('nationality'))
                id_number = self._normalize_text(record.get('id_number'))
                id_type = self._normalize_text(record.get('id_type'))
                listing_date = self._normalize_text(record.get('listing_date'))
                comments = record.get('comments')
                
                unique_hash = self._generate_hash(name, dob, id_number)
                
                cursor.execute('''
                    INSERT OR IGNORE INTO sanctions 
                    (unique_hash, full_name, date_of_birth, nationality, id_number, 
                     id_type, source, listing_date, comments, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (unique_hash, name, dob, nationality, id_number, id_type,
                      source, listing_date, comments, now, now))
                
                if cursor.rowcount > 0:
                    added += 1
                else:
                    skipped += 1
            
            conn.commit()
            logger.info(f"Batch add: {added} added, {skipped} duplicates skipped")
            
        except Exception as e:
            logger.error(f"Error in batch add: {e}")
            conn.rollback()
        finally:
            conn.close()
        
        return {"added": added, "skipped": skipped}
    
    def search_by_name(self, name: str, limit: int = 100) -> List[Dict]:
        """Search sanctions by name"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        name = self._normalize_text(name)
        
        cursor.execute('''
            SELECT * FROM sanctions 
            WHERE full_name LIKE ? AND is_active = 1
            ORDER BY full_name
            LIMIT ?
        ''', (f"%{name}%", limit))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results
    
    def get_all_sanctions(self, limit: int = 1000, offset: int = 0) -> List[Dict]:
        """Get all active sanctions"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM sanctions 
            WHERE is_active = 1
            ORDER BY full_name
            LIMIT ? OFFSET ?
        ''', (limit, offset))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results
    
    def get_sanction_by_id(self, sanction_id: int) -> Optional[Dict]:
        """Get a specific sanction by ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM sanctions WHERE id = ?', (sanction_id,))
        row = cursor.fetchone()
        conn.close()
        
        return dict(row) if row else None
    
    def delete_sanction(self, sanction_id: int) -> bool:
        """Soft delete a sanction (mark as inactive)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        cursor.execute('''
            UPDATE sanctions 
            SET is_active = 0, updated_at = ?
            WHERE id = ?
        ''', (now, sanction_id))
        
        result = cursor.rowcount > 0
        conn.commit()
        conn.close()
        
        return result
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Total records
        cursor.execute('SELECT COUNT(*) FROM sanctions WHERE is_active = 1')
        total = cursor.fetchone()[0]
        
        # By source
        cursor.execute('''
            SELECT source, COUNT(*) as count 
            FROM sanctions WHERE is_active = 1 
            GROUP BY source
        ''')
        by_source = {row[0]: row[1] for row in cursor.fetchall()}
        
        # By nationality
        cursor.execute('''
            SELECT nationality, COUNT(*) as count 
            FROM sanctions WHERE is_active = 1 AND nationality IS NOT NULL
            GROUP BY nationality
            ORDER BY count DESC
            LIMIT 10
        ''')
        by_nationality = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        return {
            "total_records": total,
            "by_source": by_source,
            "by_nationality": by_nationality
        }
    
    def log_update(self, source: str, update_type: str, added: int = 0, 
                  updated: int = 0, skipped: int = 0, status: str = "SUCCESS",
                  error_message: str = None):
        """Log an update operation"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        cursor.execute('''
            INSERT INTO update_history 
            (source, update_type, records_added, records_updated, records_skipped,
             status, error_message, executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (source, update_type, added, updated, skipped, status, error_message, now))
        
        conn.commit()
        conn.close()
    
    def get_update_history(self, limit: int = 50) -> List[Dict]:
        """Get update history"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM update_history 
            ORDER BY executed_at DESC
            LIMIT ?
        ''', (limit,))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results
    
    def clear_all(self) -> bool:
        """Clear all sanctions (use with caution)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM sanctions')
        cursor.execute('DELETE FROM update_history')
        
        conn.commit()
        conn.close()
        
        logger.warning("All sanctions cleared from database")
        return True


# Singleton instance
_db_instance = None

def get_database() -> SanctionsDatabase:
    """Get database singleton instance"""
    global _db_instance
    if _db_instance is None:
        _db_instance = SanctionsDatabase()
    return _db_instance
