# Modified from enhanced_monitor.py

from datetime import datetime
from data_manager import DataManager
from filing_scraper import FilingScraper
from filing_status_manager import FilingStatusManager
from notification_manager import NotificationManager
from transaction_extractor import TradingDataExtractor
import os
import sys
from typing import Dict, List


from dotenv import load_dotenv
load_dotenv()




class DailyRun:
    def __init__(self):
        self.data_manager = DataManager()
        self.filing_scraper = FilingScraper()
        self.status_manager = FilingStatusManager(self.data_manager)
        self.trading_data_extractor = TradingDataExtractor()
        self.notification_manager = NotificationManager()
        
    
    def run(self):
        try:
            # 1. Scrape the latest filings
            print("Step 1: Run Congressional Financial Disclosure Filing Scraper")
            scrape_result = self._run_scraper()
            print("✓ Congressional filings scraper completed")
            
            # 2. Update trading database with new filings
            print("Step 2: Updating trading database")
            self._update_trading_database()
            print("✓ Trading database updated")

            # 3. Identify pending filings
            print("Step 3: Identifying pending filings")
            pending_filings = self.status_manager.identify_pending_filings()
            pending_count = len(pending_filings)
            print(f"Found {pending_count} pending filings")
            
            # 4. Process pending PDFs (with concurrent processing)
            print("Step 4: Processing pending PDFs")
            extraction_result = self._process_pending_pdfs(pending_filings)
            print("✓ Pending PDFs processed")

        except Exception as e:
            print(f"Error during daily update: {e}")
            raise

        finally:
            # TODO: Implement notifications
            pass
        
        # TODO: Combine the outputs into a summary report
        return scrape_result
    
    def _run_scraper(self, force_full_scrape: bool = False) -> Dict:
        """
        Run the congressional filing scraper.
        
        Returns:
            Scraper results dictionary
        """
        try:
            # Note: The update_data method automatically updates the congress_filings.json file
            scraper_result = self.filing_scraper.update_data(
                force_full_scrape=force_full_scrape
            )
            return scraper_result
            
        except Exception as e:
            print(f"Filing Scraper Failed: {e}")
            return {"error": str(e)}


    def _update_trading_database(self) -> None:
        """
        Update trading database with new filings.
        
        This identifies filings that need processing and adds them
        to the pending queue, marking their status appropriately.
        """
        # Get filings that need processing
        pending_filings = self.status_manager.identify_pending_filings()
        
        # Mark filings as pending if they don't have status
        pdf_urls = [f["pdf_url"] for f in pending_filings]
                
        # Add new filings to pending processing queue
        added_count = 0
        for filing_info in pending_filings:
            # Create filing info for pending queue
            pending_info = {
                "member_name": filing_info["member_name"],
                "pdf_id": filing_info["pdf_id"],
                "pdf_url": filing_info["pdf_url"],
                "filing_type": filing_info["filing_type"],
                "year": filing_info["year"]
            }
            
            if self.data_manager.add_pending_filing(pending_info):
                added_count += 1

        print(f"Added {added_count} new filings to pending processing queue")



    def _process_pending_pdfs(self, pending_filings: List[Dict]) -> Dict:
        """
        Process pending PDF filings.
        
        Args:
            pending_filings: List of pending filing dictionaries
            
        Returns:
            Processing results summary
        """
        if not pending_filings:
            print("No pending PDFs to process")
            return {"processed": 0, "successful": 0, "failed": 0}
        
        # Limit number of files to process
        max_files_per_run = int(os.getenv("MAX_FILES_PER_RUN", 5))
        files_to_process = pending_filings[:max_files_per_run]
        print(f"Processing {len(files_to_process)} of {len(pending_filings)} pending PDFs")
        
        processed_count = 0
        successful_count = 0
        failed_count = 0
        
        try:
            self.trading_data_extractor.create_temp_dir()
            
            for filing_info in files_to_process:
                pdf_url = filing_info["pdf_url"]
                pdf_id = filing_info["pdf_id"]
                member_name = filing_info["member_name"]
                
                print(f"Processing: {member_name} - {pdf_id}")
                
                try:
                    # Download PDF
                    filename = f"{pdf_id}.pdf"
                    pdf_path = self.trading_data_extractor.download_pdf(pdf_url, filename)
                    
                    if pdf_path:
                        # Extract trading data
                        result = self.trading_data_extractor.extract_trading_data(pdf_path)
                        
                        if result.get("error"):
                            error_msg = result["error"]
                            print(f"Error extracting data from {pdf_id}: {error_msg}")
                            
                            # Determine if this is a permanent error
                            is_permanent = self._is_permanent_error(error_msg)
                            self.data_manager.mark_filing_error(pdf_url, error_msg, is_permanent)
                            
                            if is_permanent:
                                processed_count += 1
                                failed_count += 1
                        else:
                            transaction_count = len(result.get("transactions", []))
                            print(f"Found {transaction_count} transactions in {pdf_id}")
                            
                            # Mark as processed with results
                            processing_result = {
                                "pdf_url": pdf_url,
                                "member_info": result.get("member_info", {}),
                                "stock_transaction_count": transaction_count,
                                "parsed_at": result.get("parsed_at", datetime.now().isoformat()),
                                "transactions": result.get("transactions", [])
                            }
                            
                            self.data_manager.mark_filing_processed(pdf_url, processing_result)
                            
                            # Send notifications for discovered transactions
                            # if transaction_count > 0:
                            #     try:
                            #         import asyncio
                            #         asyncio.run(self._notify_transactions_discovered(
                            #             member_info=result.get("member_info", {}),
                            #             transactions=result.get("transactions", []),
                            #             filing_info=filing_info
                            #         ))
                            #     except Exception as e:
                            #         print(f"Failed to send transaction notification: {e}")
                            
                            successful_count += 1
                            processed_count += 1
                    else:
                        print(f"Failed to download PDF: {pdf_id}")
                        # Download failure is usually temporary
                        self.data_manager.mark_filing_error(pdf_url, "Failed to download PDF", is_permanent=False)
                        
                except Exception as e:
                    error_msg = str(e)
                    print(f"Error processing {pdf_id}: {error_msg}")
                    
                    # Determine if this is a permanent error
                    is_permanent = self._is_permanent_error(error_msg)
                    self.data_manager.mark_filing_error(pdf_url, error_msg, is_permanent)
                    
                    if is_permanent:
                        processed_count += 1
                        failed_count += 1
        
        finally:
            self.trading_data_extractor.cleanup_temp_dir()
        
        results = {
            "processed": processed_count,
            "successful": successful_count,
            "failed": failed_count
        }
        
        print(f"PDF Processing Summary: {successful_count} successful, {failed_count} failed")
        return results
    
    # def _save_extracted_transactions(self, filing_id: str, transactions: List[Dict]):
    #     """
    #     Save extracted transactions to trading data.
        
    #     Args:
    #         filing_id: ID of the filing
    #         transactions: List of transaction dictionaries
    #     """
    #     if not transactions:
    #         return
        
    #     trading_data = self.data_manager.load_trading_data()
        
    #     # Add transactions to the filing
    #     if 'filings' not in trading_data:
    #         trading_data['filings'] = {}
        
    #     if filing_id not in trading_data['filings']:
    #         trading_data['filings'][filing_id] = {}
        
    #     trading_data['filings'][filing_id]['transactions'] = transactions
    #     trading_data['filings'][filing_id]['transaction_count'] = len(transactions)
        
    #     # Update summary
    #     if 'summary' not in trading_data:
    #         trading_data['summary'] = {}
        
    #     total_transactions = sum(
    #         len(filing.get('transactions', []))
    #         for filing in trading_data['filings'].values()
    #     )
    #     trading_data['summary']['total_transactions'] = total_transactions
        
    #     self.data_manager.save_trading_data(trading_data)



    def _is_permanent_error(self, error_message: str) -> bool:
        """
        Determine if an error is permanent and should not be retried.
        
        Args:
            error_message: Error message to analyze
            
        Returns:
            True if error is permanent, False if transient
        """
        permanent_patterns = [
            "corrupted", "invalid format", "unsupported", 
            "malformed", "no transactions found", "file not found",
            "access denied", "invalid pdf", "permission denied"
        ]
        
        error_lower = error_message.lower()
        return any(pattern in error_lower for pattern in permanent_patterns)
    





def main():
    """Main function for enhanced daily monitoring"""
    print("Enhanced Congressional Trading Monitor")
    print("=" * 50)
    
    # Parse command line arguments (basic)
    if "--force" in sys.argv:
        #TODO: Implement force scrape logic
        force_scrape = True
        print("Force scrape enabled")
    
    
    # Create and run DailyRun instance
    daily_run = DailyRun()
    
    try:
        summary = daily_run.run()
        
        return 0
        
    except Exception as e:
        print(f"✗ Error during daily update: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
