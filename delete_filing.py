#!/usr/bin/env python3
"""
Filing Deletion Script for Congressional Trading Monitor

This script allows you to delete a specific filing and its associated 
transactions from the system, which is useful for testing notifications
with real system runs.

Usage:
    python delete_filing.py --pdf-id 20026537
    python delete_filing.py --list-processed
    python delete_filing.py --member "Allen, Hon.. Richard W." --list
"""

import json
import argparse
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
load_dotenv()

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from data_manager import DataManager


def safe_print(*args, **kwargs):
    """Print with broken pipe protection."""
    try:
        print(*args, **kwargs)
    except BrokenPipeError:
        # Handle broken pipe gracefully
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)


class FilingDeleter:
    """Utility for deleting filings and their associated transactions."""
    
    def __init__(self, data_dir: str|None = None):
        if data_dir is None:
            data_dir = os.getenv("DATA_DIR", "./data")
            
        self.data_dir = Path(data_dir)
        self.data_manager = DataManager(data_dir)
        self.congress_filings_path = self.data_dir / "congress_filings.json"
        self.trading_data_path = self.data_dir / "trading_data.json"
    
    def load_data(self) -> tuple[Dict, Dict]:
        """Load both congress filings and trading data."""
        with open(self.congress_filings_path, 'r') as f:
            congress_data = json.load(f)
        
        with open(self.trading_data_path, 'r') as f:
            trading_data = json.load(f)
        
        return congress_data, trading_data
    
    def save_data(self, congress_data: Dict, trading_data: Dict) -> None:
        """Save both datasets with timestamp updates."""
        # Update timestamps
        timestamp = datetime.now().isoformat()
        congress_data['last_updated'] = timestamp
        trading_data['last_updated'] = timestamp
        
        # Create backups
        backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        congress_backup = self.congress_filings_path.with_suffix(f'.backup.{backup_timestamp}.json')
        trading_backup = self.trading_data_path.with_suffix(f'.backup.{backup_timestamp}.json')
        
        # Save backups
        with open(congress_backup, 'w') as f:
            json.dump(congress_data, f, indent=2)
        
        with open(trading_backup, 'w') as f:
            json.dump(trading_data, f, indent=2)
        
        # Save updated files
        with open(self.congress_filings_path, 'w') as f:
            json.dump(congress_data, f, indent=2)
        
        with open(self.trading_data_path, 'w') as f:
            json.dump(trading_data, f, indent=2)
        
        print(f"✅ Data saved with backups:")
        print(f"   - {congress_backup}")
        print(f"   - {trading_backup}")
    
    def find_filing_by_pdf_id(self, pdf_id: str) -> Optional[tuple[str, Dict]]:
        """Find a filing by its PDF ID and return member_key and filing data."""
        congress_data, _ = self.load_data()
        
        for member_key, member_data in congress_data['members'].items():
            for filing in member_data['filings']:
                if filing['pdf_id'] == pdf_id:
                    return member_key, filing
        
        return None
    
    def list_processed_filings(self, member_name: Optional[str] = None) -> List[Dict]:
        """List all processed filings, optionally filtered by member."""
        congress_data, trading_data = self.load_data()
        processed_filings = []
        
        for member_key, member_data in congress_data['members'].items():
            # Filter by member if specified
            if member_name and member_name.lower() not in member_data['name'].lower():
                continue
                
            for filing in member_data['filings']:
                if filing.get('processing_status') == 'processed':
                    # Check if it has transactions
                    pdf_url = filing['pdf_link']
                    has_processed_transactions = pdf_url in trading_data.get('processed_filings', {})
                    has_filed_transactions = filing['pdf_id'] in trading_data.get('filings', {})
                    
                    processed_filings.append({
                        'pdf_id': filing['pdf_id'],
                        'member_name': member_data['name'],
                        'member_key': member_key,
                        'year': filing['year'],
                        'filing_type': filing['filing_type'],
                        'has_transactions': filing.get('has_transactions', False),
                        'has_processed_transactions': has_processed_transactions,
                        'has_filed_transactions': has_filed_transactions,
                        'processed_at': filing.get('processed_at', 'Unknown'),
                        'pdf_url': pdf_url
                    })
        
        return sorted(processed_filings, key=lambda x: x['processed_at'], reverse=True)
    
    def delete_filing(self, pdf_id: str, dry_run: bool = False) -> bool:
        """
        Delete a filing and its associated transactions.
        
        Args:
            pdf_id: The PDF ID to delete
            dry_run: If True, only show what would be deleted
            
        Returns:
            True if filing was found and deleted (or would be deleted)
        """
        congress_data, trading_data = self.load_data()
        
        # Find the filing
        result = self.find_filing_by_pdf_id(pdf_id)
        if not result:
            print(f"❌ Filing with PDF ID '{pdf_id}' not found")
            return False
        
        member_key, filing = result
        member_data = congress_data['members'][member_key]
        pdf_url = filing['pdf_link']
        
        print(f"📄 Found filing:")
        print(f"   - PDF ID: {pdf_id}")
        print(f"   - Member: {member_data['name']} ({member_data['office']})")
        print(f"   - Year: {filing['year']}")
        print(f"   - Type: {filing['filing_type']}")
        print(f"   - Status: {filing.get('processing_status', 'Unknown')}")
        print(f"   - Has transactions: {filing.get('has_transactions', False)}")
        
        # Check what data would be deleted
        filing_in_congress = True
        pending_filing_exists = any(f["pdf_id"] == pdf_id for f in trading_data.get("pending_processing", []))
        filed_transactions_exist = pdf_id in trading_data.get('processed_filings', {})
        
        print(f"\n🗑️  Data to be deleted:")
        print(f"   - Congress filing record: {'✓' if filing_in_congress else '✗'}")
        print(f"   - Pending filing: {'✓' if pending_filing_exists else '✗'}")
        print(f"   - Filed transactions: {'✓' if filed_transactions_exist else '✗'}")
        
        # Remove pending filing if it exists
        if pending_filing_exists:
            before_count = len(trading_data["pending_processing"])
            trading_data["pending_processing"] = [
                f for f in trading_data["pending_processing"] if f["pdf_id"] != pdf_id
            ]
            after_count = len(trading_data["pending_processing"])
            print(f"     └─ {before_count - after_count} pending filing(s)")
            
        
        if filed_transactions_exist:
            filed_data = trading_data['processed_filings'][pdf_id]
            transaction_count = len(filed_data.get('transactions', []))
            print(f"     └─ {transaction_count} filed transactions")
        
        if dry_run:
            print(f"\n🔍 DRY RUN: No changes made")
            return True
        
        # Confirm deletion
        print(f"\n⚠️  This will permanently delete the filing and all associated transaction data!")
        confirm = input("Are you sure you want to proceed? (yes/no): ").lower().strip()
        
        if confirm not in ['yes', 'y']:
            print("❌ Deletion cancelled")
            return False
        
        # Perform deletion
        changes_made = False
        
        # 1. Remove from congress filings
        if filing_in_congress:
            member_data['filings'] = [f for f in member_data['filings'] if f['pdf_id'] != pdf_id]
            print(f"✅ Removed filing from congress filings")
            changes_made = True
        
        # 2. Remove from processed filings
        if pending_filing_exists:
            del trading_data['processed_filings'][pdf_id]
            print(f"✅ Removed processed filing data")
            changes_made = True
        
        # 3. Remove from filed transactions
        if filed_transactions_exist:
            del trading_data['processed_filings'][pdf_id]
            print(f"✅ Removed filed transactions")
            changes_made = True
        
        # 4. Update counts
        if changes_made:
            # Update congress filings count
            total_filings = sum(len(member['filings']) for member in congress_data['members'].values())
            congress_data['total_filings'] = total_filings
            
            # Save the updated data
            self.save_data(congress_data, trading_data)
            print(f"\n🎉 Filing '{pdf_id}' successfully deleted!")
            print(f"   - Updated total filings count: {total_filings}")
            
            return True
        else:
            print(f"⚠️  No changes made - filing data not found")
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Delete congressional filings and their associated transactions"
    )
    parser.add_argument(
        '--pdf-id',
        help="PDF ID of the filing to delete (e.g., 20026537)"
    )
    parser.add_argument(
        '--list-processed',
        action='store_true',
        help="List all processed filings with transactions"
    )
    parser.add_argument(
        '--member',
        help="Filter listings by member name (partial match)"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Show what would be deleted without making changes"
    )
    parser.add_argument(
        '--data-dir',
        default=None,
        help="Directory containing the data files (default: current directory)"
    )
    
    args = parser.parse_args()
    
    deleter = FilingDeleter(args.data_dir)
    
    if args.list_processed:
        safe_print("📋 Processed Filings with Transactions:")
        safe_print("=" * 80)
        
        filings = deleter.list_processed_filings(args.member)
        
        if not filings:
            safe_print("No processed filings found.")
            return
        
        for i, filing in enumerate(filings, 1):
            safe_print(f"\n{i:2d}. PDF ID: {filing['pdf_id']}")
            safe_print(f"    Member: {filing['member_name']}")
            safe_print(f"    Year: {filing['year']} | Type: {filing['filing_type']}")
            safe_print(f"    Processed: {filing['processed_at']}")
            safe_print(f"    Transactions: Congress={filing['has_transactions']}, "
                  f"Processed={filing['has_processed_transactions']}, "
                  f"Filed={filing['has_filed_transactions']}")
        
        safe_print(f"\n📊 Total: {len(filings)} processed filings")
        
        if args.member:
            safe_print(f"🔍 Filtered by member: {args.member}")
        
        safe_print(f"\n💡 To delete a filing, run:")
        safe_print(f"   python delete_filing.py --pdf-id <PDF_ID>")
        
    elif args.pdf_id:
        success = deleter.delete_filing(args.pdf_id, dry_run=args.dry_run)
        sys.exit(0 if success else 1)
        
    else:
        parser.print_help()
        print(f"\n💡 Examples:")
        print(f"   python delete_filing.py --list-processed")
        print(f"   python delete_filing.py --member Pelosi --list-processed")
        print(f"   python delete_filing.py --pdf-id 20026537 --dry-run")
        print(f"   python delete_filing.py --pdf-id 20026537")


if __name__ == "__main__":
    main()
