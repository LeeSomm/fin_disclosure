"""
Congressional Trading Transaction Extractor
Modular extraction of trading transaction data from congressional disclosure filings.
"""

import json
import re
import os
import pdfplumber
import requests
from datetime import datetime
from pathlib import Path
import tempfile
import shutil
from typing import List, Dict, Optional, Tuple

# Add retry logic
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class TradingDataExtractor:
    """
    Extracts trading data from congressional disclosure PDFs.
    """

    # Configuration constants
    OWNER_CODES = {
        "SP": "Spouse",
        "DC": "Dependent Child", 
        "JT": "Joint"
    }
    
    AMOUNT_RANGES = [
        (15000, "$1,001 - $15,000"),
        (50000, "$15,001 - $50,000"),
        (100000, "$50,001 - $100,000"),
        (250000, "$100,001 - $250,000"),
        (500000, "$250,001 - $500,000"),
        (1000000, "$500,001 - $1,000,000"),
        (5000000, "$1,000,001 - $5,000,000"),
        (25000000, "$5,000,001 - $25,000,000"),
        (50000000, "$25,000,001 - $50,000,000"),
        (100000000, "Over $50,000,000")
    ]
    
    def __init__(self):
        self.temp_dir = None
        self.pdf_url = None

    def create_temp_dir(self):
        """Create temporary directory for PDF downloads"""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="congress_pdfs_"))
        return self.temp_dir
    
    def cleanup_temp_dir(self):
        """Remove temporary directory and all files"""
        if self.temp_dir and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout, ConnectionError))
    )
    def download_pdf(self, pdf_url: str, filename: str) -> Optional[Path]:
        """Download PDF from URL to temp directory with retry logic"""
        if not self.temp_dir:
            self.create_temp_dir()
            
        pdf_path = self.temp_dir / filename
        self.pdf_url = pdf_url
        
        try:
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            with open(pdf_path, 'wb') as f:
                f.write(response.content)
            
            # logger.info(f"Downloaded PDF: {filename}")
            return pdf_path
        except (requests.RequestException, requests.Timeout, ConnectionError) as e:
            # logger.warning(f"Retryable error downloading {pdf_url}: {e}")
            raise  # Let tenacity handle the retry
        except Exception as e:
            # logger.error(f"Non-retryable error downloading {pdf_url}: {e}")
            return None

    def _extract_member_info(self, first_page) -> Dict:
        """Extract member information from first page"""
        member_info = {}
        
        if not first_page:
            return member_info
        
        text = first_page.extract_text()
        if not text:
            return member_info
        
        # Extract filing ID
        filing_id_match = re.search(r'Filing ID #(\d+)', text)
        if filing_id_match:
            member_info['filing_id'] = filing_id_match.group(1)
        
        # Extract member name
        name_match = re.search(r'Name: (.+?)(?:\n|Status:)', text)
        if name_match:
            member_info['name'] = name_match.group(1).strip()
        
        # Extract district
        district_match = re.search(r'State/District: (.+?)(?:\n|$)', text)
        if district_match:
            member_info['district'] = district_match.group(1).strip()
            
        return member_info
    
    def _is_transaction_line(self, line: str) -> bool:
        """Check if line contains stock transaction data"""
        # print(line)
        return (('P ' in line or 'S ' in line) and 
                '$' in line and 
                any(c.isdigit() for c in line))
    
    def _is_stock_transaction(self, line: str, context_lines: List[str]) -> bool:
        """Check if line is a stock transaction based on context"""
        # Check for [ST] marker indicating stock transaction
        if '[ST]' in line:
            return True
        
        # If no ticker, check context lines for stock indicators
        for context_line in context_lines:
            if '[ST]' in context_line:
                return True
        
        return False

    def _get_context_lines(self, lines: List[str], index: int, context: int = 2) -> List[str]:
        """Get context lines around current line for asset name extraction"""
        # start = max(0, index - context)
        start = index
        end = min(len(lines), index + context + 1)
        # print(lines[start:end])
        return lines[start:end]

    def _extract_owner_code(self, line: str) -> Tuple[str, str]:
        """Extract owner code from beginning of line"""
        owner_match = re.match(r'^(SP|DC|JT)\s+(.+)', line)
        if owner_match:
            return owner_match.group(1), owner_match.group(2)
        return "", line
    
    def _get_transaction_type(self, line: str) -> str:
        """Determine transaction type from line"""
        if ' P ' in line:
            return "Purchase"
        elif ' S ' in line:
            return "Sale"
        return "Unknown"
    
    # def _extract_asset_from_context(self, line: str, context_lines: List[str], ticker: str) -> str:
    #     """Extract asset name from context lines when ticker is found"""
    #     # Extract parts from current line before transaction details
    #     line_parts = []
    #     words = line.split()
        
    #     for word in words:
    #         if any(marker in word for marker in ['P ', 'S ', '$']) or re.match(r'\d{2}/\d{2}/\d{4}', word):
    #             break
    #         line_parts.append(word)
        
    #     return ' '.join(line_parts)
    
    # def _extract_from_context_lines(self, line: str, context_lines: List[str]) -> Tuple[str, str]:
    #     """Extract ticker and asset name from context lines"""
    #     ticker_pattern = re.compile(r'\(([A-Z0-9.]+)\)')
    #     asset_parts = []
    #     ticker = ""
        
    #     # Look in subsequent lines for ticker and asset continuation
    #     for context_line in context_lines[1:]:  # Skip current line
    #         ticker_match = ticker_pattern.search(context_line)
    #         if ticker_match:
    #             ticker = ticker_match.group(1)
    #             # Add text before ticker to asset name
    #             text_before_ticker = context_line.split('(')[0].strip()
    #             if text_before_ticker:
    #                 asset_parts.append(text_before_ticker)
    #             break
            
    #         # Check if line continues asset name (no dates/amounts/transaction indicators)
    #         if not re.search(r'\d{2}/\d{2}/\d{4}|\$[\d,]+|P |S ', context_line):
    #             clean_line = re.sub(r'\[[A-Z]+\]', '', context_line).strip()
    #             if clean_line and len(clean_line) > 2:
    #                 asset_parts.append(clean_line)
        
    #     return ticker, ' '.join(asset_parts)
    
    def _clean_asset_name(self, asset_name: str) -> str:
        """Clean and normalize asset name"""
        if not asset_name:
            return ""
        
        # Remove special characters and normalize whitespace
        cleaned = re.sub(r'[^\w\s\.\-&(),]', ' ', asset_name)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Remove bracket annotations
        cleaned = re.sub(r'\s*\[[A-Z]+\]\s*', '', cleaned)
        
        return cleaned

    def _extract_asset_info(self, line: str, context_lines: List[str]) -> Tuple[str, str]:
        """Extract ticker and asset name from line and context"""
        ticker_pattern = re.compile(r'\(([A-Z0-9.]+)\)')

        ticker = ""
        ticker_line_index = -1

        # Step 1: Find the ticker in context_lines
        for i, context_line in enumerate(context_lines):
            ticker_match = ticker_pattern.search(context_line)
            if ticker_match:
                ticker = ticker_match.group(1)
                ticker_line_index = i
                break

        if not ticker:
            return "", ""

        asset_lines = []

        for j in range(ticker_line_index):
            line_text = context_lines[j]

            if j == 0:
                # Primary transaction markers (only in first line)
                for marker in ["P ", "S "]:
                    marker_index = line_text.find(marker)
                    if marker_index != -1:
                        line_text = line_text[:marker_index]
                        break
            else:
                # Secondary cutoff markers (in overflow lines, looks for dates, financial amounts - e.g., "$1", "$ 1", "- $")
                for pattern in [r'\d{2}/\d{2}/\d{4}', r'\$\d', r'\$\s*\d', r'-\s*\$']:
                    match = re.search(pattern, line_text)
                    if match:
                        line_text = line_text[:match.start()]
                        break

            asset_lines.append(line_text.strip())

        # Process final ticker line for any asset name prefix
        ticker_line_before_ticker = re.sub(rf'\s*\({re.escape(ticker)}\)\s*.*$', '', context_lines[ticker_line_index])
        asset_lines.append(ticker_line_before_ticker.strip())

        asset_name = ' '.join(asset_lines)
        return ticker, self._clean_asset_name(asset_name)

    def _extract_dates(self, line: str) -> List[str]:
        """Extract dates in MM/DD/YYYY format"""
        return re.findall(r'\d{2}/\d{2}/\d{4}', line)
    
    def _extract_and_categorize_amount(self, line: str) -> str:
        """Extract amount and convert to standard ranges"""
        amount_match = re.search(r'\$[\d,]+', line)
        if not amount_match:
            return ""
        
        try:
            # Convert to integer for range mapping
            raw_amount = amount_match.group(0).replace('$', '').replace(',', '')
            amount_value = int(raw_amount)
            
            # Map to fixed ranges
            if amount_value < 1000:
                return f"${amount_value:,}"  # Exact amount for < $1K
            
            for threshold, range_str in self.AMOUNT_RANGES:
                if amount_value <= threshold:
                    return range_str
            
            return "Over $50,000,000"
            
        except ValueError:
            return amount_match.group(0)  # Fall back to raw match

    def _parse_transaction_line(self, line: str, context_lines: List[str]) -> Optional[Dict]:
        """Parse a single transaction line with context"""
        try:
            # Extract owner code and clean line
            owner_code, line_without_owner = self._extract_owner_code(line)
            
            # Determine transaction type
            transaction_type = self._get_transaction_type(line)
            
            # Extract ticker and asset name
            ticker, asset_name = self._extract_asset_info(line, context_lines)
            
            # Extract dates
            dates = self._extract_dates(line)
            
            # Extract and categorize amount
            amount = self._extract_and_categorize_amount(line)
            
            # Validate required fields
            if not asset_name or len(dates) < 2 or not amount:
                return None
            
            return {
                "asset": asset_name.strip(),
                "ticker": ticker,
                "owner": self.OWNER_CODES.get(owner_code, "Self"),
                "owner_code": owner_code,
                "transaction_type": transaction_type,
                "transaction_date": dates[0],
                "notification_date": dates[1] if len(dates) > 1 else "",
                "amount": amount
            }
            
        except Exception as e:
            # logger.warning(f"Error parsing transaction line: {e}")
            return None

    def _extract_transactions_from_lines(self, lines: List[str]) -> List[Dict]:
        """Extract transactions from lines of text"""
        transactions = []
        
        for i, line in enumerate(lines):
            # Look for transaction lines with pattern: Asset P/S Date Date Amount
            if self._is_transaction_line(line):
                context_lines = self._get_context_lines(lines, i)
                if self._is_stock_transaction(line, context_lines):
                    transaction = self._parse_transaction_line(line, context_lines)
                
                    if transaction:
                        transactions.append(transaction)
            
        return transactions
    
    def _extract_all_transactions(self, pages) -> List[Dict]:
        """Extract transactions from all pages"""
        all_transactions = []
        
        for page in pages:
            text = page.extract_text()
            if not text:
                continue
            
            lines = text.split('\n')
            page_transactions = self._extract_transactions_from_lines(lines)
            all_transactions.extend(page_transactions)
        
        return all_transactions
    
    # def _remove_duplicates(self, transactions: List[Dict]) -> List[Dict]:
    #     """Remove duplicate transactions"""
    #     unique_transactions = []
    #     seen = set()
        
    #     for transaction in transactions:
    #         # Create key for duplicate detection
    #         key = (
    #             transaction.get('asset', ''),
    #             transaction.get('transaction_date', ''),
    #             transaction.get('amount', ''),
    #             transaction.get('owner_code', '')
    #         )
            
    #         if (key not in seen and 
    #             transaction.get('asset') and 
    #             len(transaction.get('asset', '')) > 3):
    #             seen.add(key)
    #             unique_transactions.append(transaction)
        
    #     return unique_transactions
    
    def _build_result(self, member_info: Dict, transactions: List[Dict], pdf_path: Path) -> Dict:
        """Build successful result dictionary"""
        return {
            "member_info": member_info,
            "pdf_url": self.pdf_url,
            "transactions": transactions,
            "parsed_at": datetime.now().isoformat()
        }
    
    def _build_error_result(self, error: Exception, pdf_path: Path) -> Dict:
        """Build error result dictionary"""
        return {
            "error": str(error),
            "member_info": {},
            "pdf_url": self.pdf_url,
            "transactions": [],
            "parsed_at": datetime.now().isoformat()
        }
       
    def extract_trading_data(self, pdf_path: Path) -> Dict:
        """
        Main extraction method - orchestrates the process.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Dictionary with member_info, transactions, and metadata
        """
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Extract member information from first page
                member_info = self._extract_member_info(pdf.pages[0] if pdf.pages else None)
                
                # Extract all transactions from all pages
                transactions = self._extract_all_transactions(pdf.pages)
                
                # Remove duplicates (Unnecessary)
                # unique_transactions = self._remove_duplicates(transactions)
                
                # return self._build_result(member_info, unique_transactions, pdf_path)
                return self._build_result(member_info, transactions, pdf_path)
                
        except Exception as e:
            # logger.error(f"Error extracting data from {pdf_path}: {e}")
            return self._build_error_result(e, pdf_path)

def main():
    """Main function to test the trading data extractor"""
    extractor = TradingDataExtractor()
    
    # Test with sample PDF first
    sample_pdf = Path("archive/20030461.pdf")
    if sample_pdf.exists():
        print("Testing with sample PDF...")
        result = extractor.extract_trading_data(sample_pdf)
        
        print(f"Sample Results:")
        print(f"- Member: {result['member_info'].get('name', 'Unknown')}")
        print(f"- Filing ID: {result['member_info'].get('filing_id', 'Unknown')}")
        print(f"- Transactions: {len(result['transactions'])}")
        
        if result['transactions']:
            print("Sample transactions:")
            for i, t in enumerate(result['transactions'][:3]):
                print(f"  {i+1}. {t['asset']} ({t['ticker']}) - {t['transaction_date']} - {t['amount']}")
        
        # Save sample
        with open("sample_extracted.json", "w") as f:
            json.dump(result, f, indent=2)
        print("Sample saved to sample_extracted.json")
    else:
        print("No sample PDF found at archive/20026658.pdf")


if __name__ == "__main__":
    main()


