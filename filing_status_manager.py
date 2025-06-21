"""
Filing status management for Congressional Trading Monitor.

This module handles all filing status transitions and validation,
providing a clean interface for tracking processing states.
"""

from datetime import datetime
from typing import List, Dict, Optional
from enum import Enum

class FilingStatus(Enum):
    """Enumeration of possible filing processing statuses."""
    PENDING = "pending"
    PROCESSED = "processed"
    FAILED = "failed"

class FilingStatusManager:
    """
    Handles all filing status transitions and validation.
    
    Manages the lifecycle of filings from discovery through processing,
    ensuring consistent status tracking and validation.
    """
    
    # Valid filing statuses
    STATUS_PENDING = FilingStatus.PENDING.value
    STATUS_PROCESSED = FilingStatus.PROCESSED.value
    STATUS_FAILED = FilingStatus.FAILED.value
    
    VALID_STATUSES = {STATUS_PENDING, STATUS_PROCESSED, STATUS_FAILED}
    
    def __init__(self, data_manager):
        """
        Initialize with a DataManager instance.
        
        Args:
            data_manager: DataManager for data access operations
        """
        self.data_manager = data_manager

    def identify_pending_filings(self) -> List[Dict]:
        """
        Identify all filings that need processing.
        
        Returns filings with status "pending" or no status (legacy data).
        
        Returns:
            List of filing dictionaries that need processing
        """
        congress_data = self.data_manager.load_congress_data()
        pending_filings = []
        
        for member_key, member_data in congress_data.get("members", {}).items():
            for filing in member_data.get("filings", []):
                processing_status = filing.get("processing_status")
                
                # Determine if this filing needs processing:
                # 1. No processing_status field (legacy data)
                # 2. processing_status is "pending"
                needs_processing = (
                    processing_status is None or 
                    processing_status == self.STATUS_PENDING
                )
                
                if needs_processing:
                    filing_info = {
                        "member_key": member_key,
                        "member_name": member_data["name"],
                        "pdf_url": filing["pdf_link"],
                        "pdf_id": filing["pdf_id"],
                        "filing_type": filing["filing_type"],
                        "year": filing["year"],
                        # "filing": filing  # Include full filing for updates
                    }
                    pending_filings.append(filing_info)
        
        print(f"Identified {len(pending_filings)} pending filings")
        return pending_filings
    
    def get_failed_filings(self) -> List[str]:
        """
        Get list of failed filing IDs.
        
        Returns:
            List of filing IDs that failed processing
        """
        congress_data = self.data_manager.load_congress_data()
        failed_filings = []
        
        for member_data in congress_data.get("members", {}).values():
            for filing in member_data.get("filings", []):
                if filing.get("processing_status") == self.STATUS_FAILED:
                    failed_filings.append(filing["pdf_id"])
        
        return failed_filings
    
    def update_status(self, filing_id: str, status: FilingStatus, error_message: Optional[str] = None) -> None:
        """
        Update filing status.
        
        Args:
            filing_id: Filing ID to update
            status: New FilingStatus
            error_message: Optional error message if status is FAILED
        """
        if status not in FilingStatus:
            raise ValueError(f"Invalid status: {status}")
        
        congress_data = self.data_manager.load_congress_data()
        updated = False
        
        # Find and update the filing
        for member_data in congress_data.get("members", {}).values():
            for filing in member_data.get("filings", []):
                if filing["pdf_id"] == filing_id:
                    filing["processing_status"] = status.value
                    filing["status_updated"] = datetime.now().isoformat()
                    
                    if error_message:
                        filing["error"] = error_message
                    
                    updated = True
                    break
            if updated:
                break
        
        if updated:
            self.data_manager.save_congress_data(congress_data)
            print(f"Updated filing {filing_id} status to {status.value}")
        else:
            raise KeyError(f"Filing ID {filing_id} not found in data")

    def get_status(self, filing_id: str) -> Optional[FilingStatus]:
        """
        Get current status of a filing.
        
        Args:
            filing_id: Filing ID to check
            
        Returns:
            Current FilingStatus or None if filing not found
        """
        congress_data = self.data_manager.load_congress_data()
        
        for member_data in congress_data.get("members", {}).values():
            for filing in member_data.get("filings", []):
                if filing["pdf_id"] == filing_id:
                    status_str = filing.get("processing_status")
                    if status_str:
                        try:
                            return FilingStatus(status_str)
                        except ValueError:
                            return None
                    return None
        
        return None

    def mark_filings_as_pending(self, pdf_urls: List[str]) -> int:
        """
        Mark multiple filings as pending if they have no status. New filings are set to pending by default.
        
        Args:
            pdf_urls: List of PDF URLs to mark as pending
            
        Returns:
            Number of filings updated
        """
        congress_data = self.data_manager.load_congress_data()
        updated_count = 0
        
        for member_data in congress_data.get("members", {}).values():
            for filing in member_data.get("filings", []):
                if (filing["pdf_link"] in pdf_urls and 
                    filing.get("processing_status") is None):
                    
                    filing["processing_status"] = self.STATUS_PENDING
                    filing["status_updated"] = datetime.now().isoformat()
                    updated_count += 1
        
        if updated_count > 0:
            self.data_manager.save_congress_data(congress_data)
            print(f"Marked {updated_count} filings as pending")
        
        return updated_count


    def get_status_summary(self) -> Dict[str, int]:
        """
        Get summary of filing statuses.
        
        Returns:
            Dictionary with counts for each status
        """
        congress_data = self.data_manager.load_congress_data()
        summary = {
            self.STATUS_PENDING: 0,
            self.STATUS_PROCESSED: 0,
            self.STATUS_FAILED: 0,
            "no_status": 0
        }
        
        for member_data in congress_data.get("members", {}).values():
            for filing in member_data.get("filings", []):
                status = filing.get("processing_status")
                if status in summary:
                    summary[status] += 1
                else:
                    summary["no_status"] += 1
        
        return summary





