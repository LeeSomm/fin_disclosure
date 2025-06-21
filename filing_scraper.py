import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from filing_status_manager import FilingStatus 
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Set 
import argparse

from dotenv import load_dotenv
load_dotenv()

# For retry logic
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class FilingScraper:
    def __init__(self, data_dir: str|None = None):
        # Use env var if not passed explicitly
        if data_dir is None:
            data_dir = os.getenv("DATA_DIR", "./data")
        self.data_dir = data_dir
        self.data_file = os.path.join(data_dir, "congress_filings.json")
        self.base_url = "https://disclosures-clerk.house.gov"
        self.search_url = f"{self.base_url}/FinancialDisclosure/ViewMemberSearchResult"
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
    def load_existing_data(self) -> Dict:
        """Load existing filings data from JSON file if exists."""
        if os.path.exists(self.data_file):
            with open(self.data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {
            "last_updated": None,
            "members": {}
        }
    
    def save_data(self, data: Dict) -> None:
        """Save filings data to JSON file."""
        with open(self.data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def get_existing_pdf_urls(self, data: Dict) -> Set[str]:
        """Get set of all existing PDF URLs from congress filings data."""
        existing_urls = set()
        for member_data in data.get("members", {}).values():
            for filing in member_data.get("filings", []):
                existing_urls.add(filing["pdf_link"])
        return existing_urls
    
    def get_member_key(self, name: str, office: str) -> str:
        """Generate a consistent key for a congress member."""
        clean_name = name.replace("Hon.. ", "").replace("Former Member", "").strip()
        clean_office = office.strip().replace(" ", "")
        return f"{clean_name}_{clean_office}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout, ConnectionError))
    )
    def fetch_filings(self, year: int|None = None) -> List[Dict]:
        """Scrape filings for the specified year (defaults to current year) with retry logic."""
        if year is None:
            year = datetime.now().year

        data = {"FilingYear": str(year)}

        try:
            response = requests.post(self.search_url, data=data, headers=self.headers, timeout=30)
            response.raise_for_status()
        except (requests.RequestException, requests.Timeout, ConnectionError) as e:
            print(f"Retryable network error during scraping: {e}")
            raise  # Signals to tenacity to handle the retry
        except Exception as e:
            print(f"Non-retryable error during scraping: {e}")
            raise Exception(f"Failed to fetch data: {e}")

        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr[role='row']")

        filings = []
        for row in rows:
            link_tag = row.find("a") # Checks whether there is an anchor tag in the row
            if not isinstance(link_tag, Tag):
                continue
            
            # Extract filing information
            try:
                filing_type_elem = row.find("td", {"data-label": "Filing"})
                if not filing_type_elem:
                    continue
                    
                filing_type = filing_type_elem.text.strip()
                
                # Filter for PTR filings (you can modify this filter as needed)
                if "PTR" not in filing_type:
                    continue
                
                link = self.base_url + "/" + str(link_tag["href"])
                name = link_tag.text.strip() # The text of the anchor tag is the name of the congress member
                office_elem = row.find("td", {"data-label": "Office"})
                if not isinstance(office_elem, Tag):
                    office = ""
                else:
                    office = office_elem.text.strip()
                filing_year_elem = row.find("td", {"data-label": "Filing Year"})
                if not isinstance(filing_year_elem, Tag):
                    filing_year = ""
                else:
                    filing_year = filing_year_elem.text.strip()
                
                # Extract PDF ID for unique identification
                pdf_id = link.split('/')[-1].replace('.pdf', '')
                
                filing = {
                    "pdf_id": pdf_id,
                    "name": name,
                    "office": office,
                    "year": filing_year,
                    "filing_type": filing_type,
                    "pdf_link": link,
                    "scraped_date": datetime.now().isoformat()
                }
                
                filings.append(filing)
                
            except Exception as e:
                print(f"Error parsing row: {e}")
                continue
        
        print(f"Found {len(filings)} filings")
        return filings

    
    def identify_new_filings(self, current_filings: List[Dict], existing_urls: Set[str]) -> List[Dict]:
        """Identify filings that don't already exist in the data file."""
        new_filings = []

        for filing in current_filings:
            if filing["pdf_link"] not in existing_urls:
                new_filings.append(filing)
        
        return new_filings


    def update_data(self, force_full_scrape: bool = False) -> Dict:
        """Main method to update filings data and identify new filings."""
        # Load existing data
        existing_data = self.load_existing_data()

        # Check if 12 hours have passed since last update (unless forced)
        if not force_full_scrape and existing_data.get("last_updated"):
            try:
                last_updated = datetime.fromisoformat(existing_data["last_updated"])
                time_since_last_update = datetime.now() - last_updated
                
                if time_since_last_update < timedelta(hours=12):
                    print(f"Last update was {time_since_last_update} ago (less than 12 hours). Skipping scrape.")

                    # Return summary without scraping (same as no new filings condition)
                    summary = {
                        "total_filings_found": 0,  # We didn't scrape, so 0
                        "new_filings_count": 0,
                        "new_filings": [],
                        "members_with_filings": len(existing_data.get("members", {})),
                        "last_updated": existing_data["last_updated"]
                    }
                    return summary
            except (ValueError, TypeError) as e:
                print(f"Error parsing last_updated timestamp: {e}. Proceeding with scrape.")

        existing_urls = self.get_existing_pdf_urls(existing_data)

        # Fetch current filings
        current_filings = self.fetch_filings()

        # Identify new filings
        new_filings = self.identify_new_filings(current_filings, existing_urls)

        if len(new_filings) == 0:
            print("No new filings found.")
            # Just update the timestamp
            existing_data["last_updated"] = datetime.now().isoformat()
            self.save_data(existing_data)
            
            summary = {
                "total_filings_found": len(current_filings),
                "new_filings_count": 0,
                "new_filings": [],
                "members_with_filings": len(existing_data.get("members", {})),
                "last_updated": existing_data["last_updated"]
            }
            return summary
        
        # Only add new filings to existing data structure
        print(f"Adding {len(new_filings)} new filings to existing data...")
        
        for filing in new_filings:
            member_key = self.get_member_key(filing["name"], filing["office"])

            # Initialize member if doesn't exist
            if member_key not in existing_data["members"]:
                existing_data["members"][member_key] = {
                    "name": filing["name"],
                    "office": filing["office"],
                    "filings": []
                }
        
            # Add new filing (no processing status yet)
            filing_data = {
                "pdf_id": filing["pdf_id"],
                "year": filing["year"],
                "filing_type": filing["filing_type"],
                "pdf_link": filing["pdf_link"],
                "scraped_date": filing["scraped_date"],
                "processing_status": FilingStatus.PENDING.value # Set process to PENDING for new filings
            }

            existing_data["members"][member_key]["filings"].append(filing_data)

        # Update metadata
        existing_data["last_updated"] = datetime.now().isoformat()
        existing_data["total_members"] = len(existing_data["members"])
        existing_data["total_filings"] = sum(len(member["filings"]) for member in existing_data["members"].values())

        # Save updated data
        self.save_data(existing_data)
        
        # Prepare summary
        summary = {
            "total_filings_found": len(current_filings),
            "new_filings_count": len(new_filings),
            "new_filings": new_filings,
            "members_with_filings": len(existing_data["members"]),
            "last_updated": existing_data["last_updated"]
        }
        
        return summary
    
    def print_summary(self, summary: Dict) -> None:
        """Print a summary of the scraping results."""
        print(f"\n{'='*50}")
        print(f"CONGRESS FILING SCRAPER SUMMARY")
        print(f"{'='*50}")
        print(f"Last Updated: {summary['last_updated']}")
        print(f"Total Filings Found: {summary['total_filings_found']}")
        print(f"Members with Filings: {summary['members_with_filings']}")
        print(f"New Filings: {summary['new_filings_count']}")
        
        if summary['new_filings']:
            print(f"\nNEW FILINGS DETECTED:")
            print(f"{'-'*30}")
            for filing in summary['new_filings'][:5]:  # Show first 5
                print(f"• {filing['name']} ({filing['office']}) - {filing['filing_type']}")
            
            if len(summary['new_filings']) > 5:
                print(f"... and {len(summary['new_filings']) - 5} more")
        else:
            print("\nNo new filings detected.")
        
        print(f"{'='*50}\n")


def main():
    """Main function for running the scraper."""
    parser = argparse.ArgumentParser(description="Scrape congressional financial disclosure filings")
    parser.add_argument("--force", action="store_true", 
                       help="Force a full scrape, bypassing the 12-hour update check")
    args = parser.parse_args()
    
    scraper = FilingScraper()
    
    try:
        # Update data and get summary
        summary = scraper.update_data(force_full_scrape=args.force)
        
        # Print summary
        scraper.print_summary(summary)
        
        # If there are new filings, you could add notification logic here
        if summary['new_filings_count'] > 0:
            print(f"🚨 {summary['new_filings_count']} new filings detected!")
            # You can add your notification logic here (email, webhook, etc.)
            
    except Exception as e:
        print(f"Error running scraper: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())