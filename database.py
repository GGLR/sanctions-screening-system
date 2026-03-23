"""
Sanctions Database Module - GitHub Version
SQLite-based storage for sanctions list data
"""

import sqlite3
import logging
import hashlib
import os
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# GitHub version uses local database
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sanctions.db")


class SanctionsDatabase:
    """SQLite database manager for sanctions list"""
    
    def __init__(self, db_path: str = None):
        """Initialize database connection"""
        self.db_path = db_path or DB_PATH
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
        
        # Run migrations to add missing columns
        self._run_migrations()
        
        logger.info(f"Database initialized at {self.db_path}")
    
    def _run_migrations(self):
        """Run database migrations to add missing columns"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Get existing columns
            cursor.execute("PRAGMA table_info(sanctions)")
            columns = [col[1] for col in cursor.fetchall()]
            
            # Add id_number_2 column if missing
            if 'id_number_2' not in columns:
                cursor.execute("ALTER TABLE sanctions ADD COLUMN id_number_2 TEXT")
                logger.info("Added id_number_2 column")
            
            # Add id_type_2 column if missing
            if 'id_type_2' not in columns:
                cursor.execute("ALTER TABLE sanctions ADD COLUMN id_type_2 TEXT")
                logger.info("Added id_type_2 column")
            
            conn.commit()
        except Exception as e:
            logger.warning(f"Migration error (may be OK): {e}")
        finally:
            conn.close()
    
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
    
    def _generate_hash(self, data: Dict) -> str:
        """Generate unique hash for a record"""
        hash_input = f"{data.get('full_name', '')}{data.get('date_of_birth', '')}{data.get('nationality', '')}"
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def add_sanction(self, data: Dict) -> bool:
        """Add a single sanction record"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Generate unique hash
            unique_hash = self._generate_hash(data)
            
            # Normalize fields
            full_name = self._normalize_text(data.get('full_name'))
            date_of_birth = self._normalize_text(data.get('date_of_birth'))
            nationality = self._normalize_text(data.get('nationality'))
            id_number = self._normalize_text(data.get('id_number'))
            id_type = self._normalize_text(data.get('id_type'))
            source = data.get('source', 'UNKNOWN')
            
            now = datetime.now().isoformat()
            
            cursor.execute('''
                INSERT OR REPLACE INTO sanctions 
                (unique_hash, full_name, date_of_birth, nationality, id_number, id_type, 
                 source, listing_date, comments, created_at, updated_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ''', (unique_hash, full_name, date_of_birth, nationality, id_number, id_type,
                  source, data.get('listing_date'), data.get('comments'), now, now))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error adding sanction: {e}")
            return False
    
    def add_sanctions_batch(self, sanctions_list: List[Dict]) -> Dict:
        """Add multiple sanction records"""
        stats = {'added': 0, 'updated': 0, 'skipped': 0}
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for data in sanctions_list:
                try:
                    unique_hash = self._generate_hash(data)
                    full_name = self._normalize_text(data.get('full_name'))
                    date_of_birth = self._normalize_text(data.get('date_of_birth'))
                    nationality = self._normalize_text(data.get('nationality'))
                    id_number = self._normalize_text(data.get('id_number'))
                    id_type = self._normalize_text(data.get('id_type'))
                    source = data.get('source', 'UNKNOWN')
                    
                    now = datetime.now().isoformat()
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO sanctions 
                        (unique_hash, full_name, date_of_birth, nationality, id_number, id_type, 
                         source, listing_date, comments, created_at, updated_at, is_active)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    ''', (unique_hash, full_name, date_of_birth, nationality, id_number, id_type,
                          source, data.get('listing_date'), data.get('comments'), now, now))
                    
                    stats['added'] += 1
                except sqlite3.IntegrityError:
                    stats['skipped'] += 1
                except Exception as e:
                    logger.debug(f"Error processing record: {e}")
                    stats['skipped'] += 1
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Batch insert error: {e}")
        
        return stats
    
    def get_sanction_by_id(self, sanction_id: int) -> Optional[Dict]:
        """Get a single sanction by ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM sanctions WHERE id = ?', (sanction_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return dict(row)
        return None
    
    def get_all_sanctions(self, limit: int = None) -> List[Dict]:
        """Get all active sanctions"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if limit:
            cursor.execute('SELECT * FROM sanctions WHERE is_active = 1 LIMIT ?', (limit,))
        else:
            cursor.execute('SELECT * FROM sanctions WHERE is_active = 1')
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def search_by_name(self, name: str, exact: bool = False) -> List[Dict]:
        """Search sanctions by name"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        name = self._normalize_text(name)
        
        if exact:
            cursor.execute('SELECT * FROM sanctions WHERE full_name = ? AND is_active = 1', (name,))
        else:
            cursor.execute('SELECT * FROM sanctions WHERE full_name LIKE ? AND is_active = 1', (f'%{name}%',))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_sanctions_by_source(self, source: str) -> List[Dict]:
        """Get sanctions by source (e.g., UN, MOHA)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM sanctions WHERE source = ? AND is_active = 1', (source.upper(),))
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Total active sanctions
        cursor.execute('SELECT COUNT(*) as count FROM sanctions WHERE is_active = 1')
        total = cursor.fetchone()['count']
        
        # By source
        cursor.execute('SELECT source, COUNT(*) as count FROM sanctions WHERE is_active = 1 GROUP BY source')
        sources = {row['source']: row['count'] for row in cursor.fetchall()}
        
        conn.close()
        
        return {
            'total': total,
            'by_source': sources
        }
    
    def clear_source(self, source: str) -> int:
        """Clear all sanctions from a specific source"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('UPDATE sanctions SET is_active = 0 WHERE source = ?', (source.upper(),))
        deleted = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        return deleted
    
    def delete_all(self) -> bool:
        """Delete all sanctions (use with caution)"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM sanctions')
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error deleting all sanctions: {e}")
            return False


# Singleton instance
_db_instance = None

def get_database() -> SanctionsDatabase:
    """Get database singleton instance"""
    global _db_instance
    if _db_instance is None:
        _db_instance = SanctionsDatabase()
    return _db_instance


def reset_database():
    """Reset database instance (for testing)"""
    global _db_instance
    _db_instance = None