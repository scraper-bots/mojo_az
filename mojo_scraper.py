"""
Mojo.az User Scraper
Scrapes user data from mojo.az with phone validation and exports to CSV/XLSX/JSON
"""
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import csv
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import re
from pathlib import Path
import logging
from phone_validator import PhoneValidator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MojoScraper:
    """Scrapes user data from mojo.az"""

    def __init__(self, start_id: int = 1, end_id: int = 47000, max_concurrent: int = 200,
                 checkpoint_file: str = 'scraper_checkpoint.json'):
        """
        Initialize scraper

        Args:
            start_id: Starting user ID
            end_id: Ending user ID
            max_concurrent: Maximum concurrent requests (increased default to 200)
            checkpoint_file: File to save progress for crash recovery
        """
        self.base_url = "https://mojo.az/az/users/{}"
        self.start_id = start_id
        self.end_id = end_id
        self.max_concurrent = max_concurrent
        self.checkpoint_file = checkpoint_file
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.results: List[Dict] = []
        self.session: Optional[aiohttp.ClientSession] = None

        # Statistics
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'invalid_phone': 0,
            'no_phone': 0,
            'valid_users': 0,
            'last_processed_id': start_id - 1
        }

    async def create_session(self):
        """Create aiohttp session with proper headers and connection pooling"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'az,en;q=0.9',
            'Connection': 'keep-alive',
        }
        # Reduced timeout for faster failures, increased connection limit
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        connector = aiohttp.TCPConnector(limit=300, limit_per_host=100, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout, connector=connector)

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()

    def save_checkpoint(self):
        """Save current progress to checkpoint file"""
        checkpoint_data = {
            'results': self.results,
            'stats': self.stats,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
        logger.info(f"✓ Checkpoint saved: {len(self.results)} users")

    def load_checkpoint(self) -> bool:
        """
        Load progress from checkpoint file

        Returns:
            True if checkpoint loaded, False otherwise
        """
        checkpoint_path = Path(self.checkpoint_file)
        if not checkpoint_path.exists():
            return False

        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)

            self.results = checkpoint_data.get('results', [])
            self.stats = checkpoint_data.get('stats', self.stats)

            logger.info(f"✓ Checkpoint loaded: {len(self.results)} users from {checkpoint_data.get('timestamp')}")
            logger.info(f"  Resuming from ID: {self.stats.get('last_processed_id', self.start_id)}")
            return True
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return False

    def parse_user_data(self, html: str, user_id: int) -> Optional[Dict]:
        """
        Parse user data from HTML

        Args:
            html: HTML content
            user_id: User ID

        Returns:
            Dictionary with user data or None if invalid
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Find the h2 tag with user name first (more specific)
            name_tag = soup.find('h2', {'class': 'pb-0'})

            if not name_tag:
                logger.debug(f"User {user_id}: No name tag found")
                return None

            # Get the name
            name = name_tag.get_text(strip=True)

            if not name:
                logger.debug(f"User {user_id}: Empty name")
                return None

            # Get the parent div containing user info
            user_div = name_tag.find_parent('div', class_=lambda x: x and 'p-2' in x.split())

            if not user_div:
                logger.debug(f"User {user_id}: No user div found")
                return None

            # Extract phone number - search in all text content
            phone_raw = None
            text_content = user_div.get_text()

            # Look for phone pattern in the text
            phone_match = re.search(r'\(?\d{3}\)?\s*\d{3}[-\s]?\d{2}[-\s]?\d{2}', text_content)
            if phone_match:
                phone_raw = phone_match.group(0)

            # Validate phone number
            if not phone_raw:
                self.stats['no_phone'] += 1
                logger.debug(f"User {user_id}: No phone found")
                return None

            validated_phone = PhoneValidator.validate_phone(phone_raw)
            if not validated_phone:
                self.stats['invalid_phone'] += 1
                logger.debug(f"Invalid phone for user {user_id}: {phone_raw}")
                return None

            # Extract other fields
            registration_date = None
            last_seen_date = None
            listing_count = None

            # Extract registration date
            reg_match = re.search(r'Qeydiyyat tarixi:\s*(.+?)(?:\n|<br)', text_content, re.DOTALL)
            if reg_match:
                registration_date = reg_match.group(1).strip()

            # Extract last seen date
            seen_match = re.search(r'Saytda olduğu tarix:\s*(.+?)(?:\n|<br)', text_content, re.DOTALL)
            if seen_match:
                last_seen_date = seen_match.group(1).strip()

            # Extract listing count
            listing_match = re.search(r'Elan sayı:\s*(\d+)', text_content)
            if listing_match:
                listing_count = int(listing_match.group(1))

            return {
                'user_id': user_id,
                'name': name,
                'phone': validated_phone,
                'registration_date': registration_date,
                'last_seen_date': last_seen_date,
                'listing_count': listing_count,
                'url': self.base_url.format(user_id),
                'scraped_at': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error parsing user {user_id}: {e}")
            return None

    async def fetch_user(self, user_id: int) -> Optional[Dict]:
        """
        Fetch and parse single user

        Args:
            user_id: User ID to fetch

        Returns:
            User data dictionary or None
        """
        async with self.semaphore:
            url = self.base_url.format(user_id)

            try:
                async with self.session.get(url) as response:
                    self.stats['total_processed'] += 1

                    if response.status == 200:
                        html = await response.text()
                        user_data = self.parse_user_data(html, user_id)

                        if user_data:
                            self.stats['successful'] += 1
                            self.stats['valid_users'] += 1
                            self.stats['last_processed_id'] = user_id
                            logger.info(f"✓ User {user_id}: {user_data['name']} - {user_data['phone']}")
                            return user_data
                        else:
                            self.stats['failed'] += 1
                            self.stats['last_processed_id'] = user_id
                            return None
                    else:
                        self.stats['failed'] += 1
                        self.stats['last_processed_id'] = user_id
                        logger.debug(f"✗ User {user_id}: HTTP {response.status}")
                        return None

            except asyncio.TimeoutError:
                self.stats['failed'] += 1
                self.stats['last_processed_id'] = user_id
                logger.debug(f"⏱ User {user_id}: Timeout")
                return None
            except Exception as e:
                self.stats['failed'] += 1
                self.stats['last_processed_id'] = user_id
                logger.debug(f"✗ User {user_id}: {e}")
                return None

    async def scrape_batch(self, batch_ids: List[int]) -> List[Dict]:
        """
        Scrape a batch of user IDs

        Args:
            batch_ids: List of user IDs

        Returns:
            List of valid user data
        """
        tasks = [self.fetch_user(user_id) for user_id in batch_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Filter out None and exceptions
        return [r for r in results if r is not None and not isinstance(r, Exception)]

    async def scrape_all(self, batch_size: int = 5000, save_every: int = 2000):
        """
        Scrape all users from start_id to end_id with auto-save checkpoints

        Args:
            batch_size: Number of IDs to process in each batch (increased for speed)
            save_every: Save checkpoint every N users processed
        """
        # Try to load checkpoint
        checkpoint_loaded = self.load_checkpoint()

        # Calculate starting point
        resume_id = self.stats.get('last_processed_id', self.start_id - 1) + 1
        if resume_id > self.start_id:
            logger.info(f"Resuming from user ID: {resume_id}")
            actual_start = resume_id
        else:
            actual_start = self.start_id

        await self.create_session()

        try:
            total_ids = self.end_id - actual_start + 1
            logger.info(f"Starting scrape: {actual_start} to {self.end_id} ({total_ids} users)")
            logger.info(f"Max concurrent requests: {self.max_concurrent}")
            logger.info(f"Auto-save checkpoint every {save_every} users")

            last_save_count = self.stats['total_processed']

            # Process in batches
            for batch_start in range(actual_start, self.end_id + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, self.end_id)
                batch_ids = list(range(batch_start, batch_end + 1))

                logger.info(f"\nProcessing batch: {batch_start} to {batch_end}")

                batch_results = await self.scrape_batch(batch_ids)
                self.results.extend(batch_results)

                # Log progress
                logger.info(f"Batch complete. Valid users in batch: {len(batch_results)}")
                logger.info(f"Total progress: {self.stats['total_processed']}/{total_ids + (actual_start - self.start_id)} "
                           f"({(self.stats['total_processed']/(self.end_id - self.start_id + 1)*100):.1f}%)")
                logger.info(f"Total valid users collected: {self.stats['valid_users']}")

                # Auto-save checkpoint
                if self.stats['total_processed'] - last_save_count >= save_every:
                    self.save_checkpoint()
                    last_save_count = self.stats['total_processed']

            # Final save
            self.save_checkpoint()

            # Print completion summary
            logger.info("\n" + "="*60)
            logger.info("SCRAPING COMPLETE")
            logger.info("="*60)
            logger.info(f"Total processed: {self.stats['total_processed']}")
            logger.info(f"Valid users saved: {self.stats['valid_users']}")
            logger.info(f"Failed requests: {self.stats['failed']}")
            logger.info(f"No phone found: {self.stats['no_phone']}")
            logger.info(f"Invalid phone: {self.stats['invalid_phone']}")
            logger.info("="*60)

        except KeyboardInterrupt:
            logger.warning("\n⚠ Scraping interrupted by user (Ctrl+C)")
            self.save_checkpoint()
            logger.info(f"Progress saved: {self.stats['valid_users']} users collected")
            # Don't re-raise - exit gracefully
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            self.save_checkpoint()
            raise
        finally:
            await self.close_session()

    def export_to_csv(self, filename: str = 'mojo_users.csv'):
        """Export results to CSV"""
        if not self.results:
            logger.warning("No results to export to CSV")
            return

        filepath = Path(filename)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            if self.results:
                writer = csv.DictWriter(f, fieldnames=self.results[0].keys())
                writer.writeheader()
                writer.writerows(self.results)

        logger.info(f"✓ Exported to CSV: {filepath.absolute()}")

    def export_to_xlsx(self, filename: str = 'mojo_users.xlsx'):
        """Export results to Excel"""
        if not self.results:
            logger.warning("No results to export to XLSX")
            return

        filepath = Path(filename)
        df = pd.DataFrame(self.results)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logger.info(f"✓ Exported to XLSX: {filepath.absolute()}")

    def export_to_json(self, filename: str = 'mojo_users.json'):
        """Export results to JSON"""
        if not self.results:
            logger.warning("No results to export to JSON")
            return

        filepath = Path(filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        logger.info(f"✓ Exported to JSON: {filepath.absolute()}")

    def export_all(self, base_filename: str = 'mojo_users'):
        """Export to all formats"""
        self.export_to_csv(f'{base_filename}.csv')
        self.export_to_xlsx(f'{base_filename}.xlsx')
        self.export_to_json(f'{base_filename}.json')


async def main():
    """Main function with optimized settings"""
    # Configuration - OPTIMIZED FOR SPEED
    START_ID = 1
    END_ID = 47000
    MAX_CONCURRENT = 200  # Increased for faster scraping
    BATCH_SIZE = 5000     # Larger batches for efficiency
    SAVE_EVERY = 2000     # Save checkpoint every 2000 users

    logger.info("="*60)
    logger.info("MOJO.AZ SCRAPER - OPTIMIZED VERSION")
    logger.info("="*60)
    logger.info(f"Speed optimizations:")
    logger.info(f"  - Concurrent requests: {MAX_CONCURRENT}")
    logger.info(f"  - Batch size: {BATCH_SIZE}")
    logger.info(f"  - Connection timeout: 10s (fast fail)")
    logger.info(f"  - Auto-save every: {SAVE_EVERY} users")
    logger.info(f"  - Crash recovery: Enabled")
    logger.info("="*60)

    # Create scraper
    scraper = MojoScraper(
        start_id=START_ID,
        end_id=END_ID,
        max_concurrent=MAX_CONCURRENT
    )

    # Scrape all users with auto-checkpoint
    await scraper.scrape_all(batch_size=BATCH_SIZE, save_every=SAVE_EVERY)

    # Export results
    logger.info("\nExporting results to files...")
    scraper.export_all('mojo_users')

    logger.info("\n" + "="*60)
    logger.info("ALL DONE!")
    logger.info("="*60)


if __name__ == "__main__":
    # Run the scraper
    asyncio.run(main())
