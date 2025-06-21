"""
Centralized data access layer for Congressional Trading Monitor.

This module provides a unified interface for all JSON file operations,
ensuring consistent error handling, atomic writes, and data validation.
"""

import os
import json
import shutil
from datetime import datetime
from pathlib import Path
import tempfile
from typing import Dict, List, Set, Optional

from dotenv import load_dotenv

load_dotenv()


class DataManager:
    """
    Centralized data access for all JSON files.
    
    Provides atomic operations, consistent error handling,
    and validation for congress_filings.json and trading_data.json.
    """

    def __init__(self, data_dir: str|None = None):
        """
        Initialize DataManager with specified data directory.

        Args:
            data_dir: Directory where JSON files are stored
        """
        # Use env var if not passed explicitly
        if data_dir is None:
            data_dir = os.getenv("DATA_DIR", "./data")

        self.data_dir = Path(data_dir)
        self.congress_file = self.data_dir / "congress_filings.json"
        self.trading_file = self.data_dir / "trading_data.json"
        
        # Ensure data directory exists
        self.data_dir.mkdir(exist_ok=True)


    def load_congress_data(self) -> Dict:
        """
        Load congressional filings data.
        
        Returns:
            Dictionary containing congress filings data with structure:
            {
                "last_updated": str,
                "total_members": int,
                "total_filings": int,
                "members": {
                    "member_key": {
                        "name": str,
                        "office": str,
                        "filings": [...]
                    }
                }
            }
        """
        if not self.congress_file.exists():
            return {
                "last_updated": None,
                "total_members": 0,
                "total_filings": 0,
                "members": {}
            }
        
        try:
            with open(self.congress_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Validate basic structure
            if not isinstance(data, dict) or "members" not in data:
                raise ValueError("Invalid congress data structure")

            
            # Ensure required fields exist
            if "total_members" not in data:
                data["total_members"] = len(data.get("members", {}))
            if "total_filings" not in data:
                data["total_filings"] = sum(
                    len(member.get("filings", [])) 
                    for member in data.get("members", {}).values()
                )
            return data
            
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON format in congress data file")
            
        except Exception as e:
            raise RuntimeError("Failed to load congress data") from e


    def _atomic_write(self, file_path: Path, data: Dict) -> None:
        """
        Write JSON data atomically (write to temp file, then rename).
        
        Args:
            file_path: Target file path
            data: Data to write
        """
        # for k, v in data.items():
        #     print(f"{k}: {type(v)}")
        # Create backup if file exists
        # if file_path.exists():
        #     backup_path = file_path.with_suffix(f".backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        #     shutil.copy2(file_path, backup_path)
        
        # Create temp file in the same directory as the target
        dir_path = file_path.parent
        
        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=dir_path, delete=False) as tmp_file:
                json.dump(data, tmp_file, indent=2, ensure_ascii=False)
                temp_path = Path(tmp_file.name)

            # Perform atomic replace
            os.replace(temp_path, file_path)  # atomic if on same filesystem

        except Exception as e:
            # Clean up temp file on error
            if 'temp_path' in locals() and temp_path.exists():
                temp_path.unlink()
            raise e
            
    def save_congress_data(self, data: Dict) -> None:
        """
        Save congressional filings data atomically with validation.
        
        Args:
            data: Congress filings data to save
        """
        # Validate data structure
        if not isinstance(data, dict) or "members" not in data:
            raise ValueError("Invalid congress data structure")
        
        # Update metadata
        data["total_members"] = len(data.get("members", {}))
        data["total_filings"] = sum(
            len(member.get("filings", [])) 
            for member in data.get("members", {}).values()
        )
        data["last_updated"] = datetime.now().isoformat()
        self._atomic_write(self.congress_file, data)


    def load_trading_data(self) -> Dict:
        """
        Load trading data.
        
        Returns:
            Dictionary containing trading data with structure:
            {
                "last_updated": str,
                "pending_processing": [...],
                "summary": {...},
                "processed_filings": {...}
            }
        """
        if not self.trading_file.exists():
            return {
                "last_updated": "",
                "pending_processing": [],
                "summary": {
                    "total_pdfs": 0,
                    "processed_pdfs": 0,
                    "pending_pdfs": 0
                },
                "processed_filings": {}
            }
        
        try:
            with open(self.trading_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Validate basic structure
            required_fields = ["pending_processing", "processed_filings", "summary"]
            for field in required_fields:
                if field not in data:
                    data[field] = [] if field == "pending_processing" else {}
            
            # Ensure summary has required fields
            summary = data.get("summary", {})
            summary.setdefault("total_pdfs", 0)
            summary.setdefault("processed_pdfs", len(data.get("processed_filings", {})))
            summary.setdefault("pending_pdfs", len(data.get("pending_processing", [])))
            data["summary"] = summary
            
            return data
            
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON format in trading data file") from e
        except Exception as e:
            raise

    def save_trading_data(self, data: Dict) -> None:
        """
        Save trading data atomically with validation.
        
        Args:
            data: Trading data to save
        """        
        # Update summary
        summary = data.setdefault("summary", {})
        summary["processed_pdfs"] = len(data.get("processed_filings", {}))
        summary["pending_pdfs"] = len(data.get("pending_processing", []))
        data["last_updated"] = datetime.now().isoformat()
        
        self._atomic_write(self.trading_file, data)

    def get_pending_filings(self) -> List[Dict]:
        """
        Get list of filings pending processing.
        
        Returns:
            List of pending filing dictionaries
        """
        trading_data = self.load_trading_data()
        return trading_data.get("pending_processing", [])
    
    def get_existing_pdf_urls(self) -> Set[str]:
        """
        Get set of all existing PDF URLs from congress filings data.
        
        Returns:
            Set of PDF URLs that have been scraped
        """
        congress_data = self.load_congress_data()
        existing_urls = set()
        
        for member_data in congress_data.get("members", {}).values():
            for filing in member_data.get("filings", []):
                existing_urls.add(filing["pdf_link"])
        
        return existing_urls
    
    def mark_filing_processed(self, pdf_url: str, result: Dict) -> None:
        """
        Mark a filing as successfully processed.
        
        Args:
            pdf_url: The PDF URL that was processed
            result: The processing result to store
        """
        # Load current data
        trading_data = self.load_trading_data()
        congress_data = self.load_congress_data()
        
        # Update processing status in congress_filings.json (minimal data only)
        congress_updated = False
        for member_key, member_data in congress_data["members"].items():
            for filing in member_data["filings"]:
                if filing["pdf_link"] == pdf_url:
                    filing["processing_status"] = "processed"
                    filing["processed_at"] = datetime.now().isoformat()
                    # Only store minimal success indicator, not full transaction data
                    if result and not result.get("error"):
                        filing["has_stock_transactions"] = result.get("transaction_count", 0) > 0
                    congress_updated = True
                    break
            if congress_updated:
                break
        
        # Remove from pending queue
        trading_data["pending_processing"] = [
            item for item in trading_data["pending_processing"] 
            if item["pdf_url"] != pdf_url
        ]
        
        # Add full processing result to trading_data.json only
        if result:
            filing_no = pdf_url.split("/")[-1].replace(".pdf", "")
            trading_data["processed_filings"][filing_no] = result
            # trading_data["processed_filings"][pdf_url] = {
            #     "processed_at": datetime.now().isoformat(),
            #     "result": result
            # }
        
        # Save updated data
        if congress_updated:
            self.save_congress_data(congress_data)
        self.save_trading_data(trading_data)
        
    
    def mark_filing_error(self, pdf_url: str, error_message: str, is_permanent: bool = False) -> None:
        """
        Mark a filing as having an error.
        
        Args:
            pdf_url: The PDF URL that had an error
            error_message: Description of the error
            is_permanent: Whether this is a permanent error that shouldn't be retried
        """
        trading_data = self.load_trading_data()
        
        if is_permanent:
            # Remove from pending and mark as processed with error
            trading_data["pending_processing"] = [
                item for item in trading_data["pending_processing"] 
                if item["pdf_url"] != pdf_url
            ]
            
            trading_data["processed_filings"][pdf_url] = {
                "processed_at": datetime.now().isoformat(),
                "result": {"error": error_message, "permanent": True}
            }
            
            # Update congress_filings.json for permanent errors
            congress_data = self.load_congress_data()
            congress_updated = False
            for member_key, member_data in congress_data["members"].items():
                for filing in member_data["filings"]:
                    if filing["pdf_link"] == pdf_url:
                        filing["processing_status"] = "failed"
                        filing["failed_at"] = datetime.now().isoformat()
                        filing["error"] = error_message
                        congress_updated = True
                        break
                if congress_updated:
                    break
            
            if congress_updated:
                self.save_congress_data(congress_data)
            
        else:
            # Keep in pending queue for retry, just log the error
            print(f"Temporary error for {pdf_url}: {error_message} (will retry)")
        
        # Save trading data
        self.save_trading_data(trading_data)
    
    def add_pending_filing(self, filing_info: Dict) -> bool:
        """
        Add a filing to the pending processing queue.
        
        Args:
            filing_info: Filing information dictionary
            
        Returns:
            True if added, False if already exists
        """
        trading_data = self.load_trading_data()
        
        # Check if already in pending queue
        existing_urls = {item["pdf_url"] for item in trading_data["pending_processing"]}
        if filing_info["pdf_url"] in existing_urls:
            return False
        
        # Add discovered timestamp
        filing_info["discovered_at"] = datetime.now().isoformat()
        trading_data["pending_processing"].append(filing_info)
        
        self.save_trading_data(trading_data)
        return True
    
    def get_last_update_time(self) -> Optional[datetime]:
        """
        Get the last update time from congress data.
        
        Returns:
            Last update datetime or None if never updated
        """
        congress_data = self.load_congress_data()
        last_updated = congress_data.get("last_updated")
        
        if last_updated:
            try:
                return datetime.fromisoformat(last_updated)
            except (ValueError, TypeError):
                print(f"Invalid last_updated timestamp: {last_updated}")
        
        return None
    