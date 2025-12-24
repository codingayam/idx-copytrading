"""
Broker Data Crawler for NeoBDM Broker Stalker

This module crawls broker trading data (buy/sell tables) from the authenticated
Plotly Dash application at https://neobdm.tech/broker_stalker/

Usage:
    python -m src.services.broker_crawler

Environment Variables:
    NEOBDM_USERNAME: Login username for neobdm.tech
    NEOBDM_PASSWORD: Login password for neobdm.tech
"""

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Complete list of 90 broker codes
BROKER_CODES: list[dict[str, str]] = [
    {"code": "AD", "name": "OSO Sekuritas Indonesia"},
    {"code": "AF", "name": "Harita Kencana Sekuritas"},
    {"code": "AG", "name": "Kiwoom Sekuritas Indonesia"},
    {"code": "AH", "name": "Shinhan Sekuritas Indonesia"},
    {"code": "AI", "name": "UOB Kay Hian Sekuritas"},
    {"code": "AK", "name": "UBS Sekuritas Indonesia"},
    {"code": "AN", "name": "Wanteg Sekuritas"},
    {"code": "AO", "name": "ERDIKHA ELIT SEKURITAS"},
    {"code": "AP", "name": "Pacific Sekuritas Indonesia"},
    {"code": "AR", "name": "Binaartha Sekuritas"},
    {"code": "AT", "name": "Phintraco Sekuritas"},
    {"code": "AZ", "name": "Sucor Sekuritas"},
    {"code": "BB", "name": "Verdhana Sekuritas Indonesia"},
    {"code": "BF", "name": "Inti Fikasa Sekuritas"},
    {"code": "BK", "name": "J.P. Morgan Sekuritas Indonesia"},
    {"code": "BQ", "name": "Korea Investment and Sekuritas Indonesia"},
    {"code": "BR", "name": "Trust Sekuritas"},
    {"code": "BS", "name": "Equity Sekuritas Indonesia"},
    {"code": "CC", "name": "MANDIRI SEKURITAS"},
    {"code": "CD", "name": "Mega Capital Sekuritas"},
    {"code": "CP", "name": "KB Valbury Sekuritas"},
    {"code": "DD", "name": "Makindo Sekuritas"},
    {"code": "DH", "name": "SINARMAS SEKURITAS"},
    {"code": "DP", "name": "DBS Vickers Sekuritas Indonesia"},
    {"code": "DR", "name": "RHB Sekuritas Indonesia"},
    {"code": "DU", "name": "KAF Sekuritas Indonesia"},
    {"code": "DX", "name": "Bahana Sekuritas"},
    {"code": "EL", "name": "Evergreen Sekuritas Indonesia"},
    {"code": "EP", "name": "MNC Sekuritas"},
    {"code": "ES", "name": "EKOKAPITAL SEKURITAS"},
    {"code": "FO", "name": "Forte Global Sekuritas"},
    {"code": "FS", "name": "Yuanta Sekuritas Indonesia"},
    {"code": "FZ", "name": "Waterfront Sekuritas Indonesia"},
    {"code": "GA", "name": "BNC Sekuritas Indonesia"},
    {"code": "GI", "name": "Webull Sekuritas Indonesia"},
    {"code": "GR", "name": "PANIN SEKURITAS Tbk."},
    {"code": "GW", "name": "HSBC Sekuritas Indonesia"},
    {"code": "HD", "name": "KGI Sekuritas Indonesia"},
    {"code": "HP", "name": "Henan Putihrai Sekuritas"},
    {"code": "IC", "name": "Integrity Capital Sekuritas"},
    {"code": "ID", "name": "Anugerah Sekuritas Indonesia"},
    {"code": "IF", "name": "SAMUEL SEKURITAS INDONESIA"},
    {"code": "IH", "name": "Indo Harvest Sekuritas"},
    {"code": "II", "name": "Danatama Makmur Sekuritas"},
    {"code": "IN", "name": "INVESTINDO NUSANTARA SEKURITA"},
    {"code": "IT", "name": "INTI TELADAN SEKURITAS"},
    {"code": "IU", "name": "Indo Capital Sekuritas"},
    {"code": "KI", "name": "Ciptadana Sekuritas Asia"},
    {"code": "KK", "name": "Phillip Sekuritas Indonesia"},
    {"code": "KZ", "name": "CLSA Sekuritas Indonesia"},
    {"code": "LG", "name": "Trimegah Sekuritas Indonesia Tbk."},
    {"code": "LS", "name": "Reliance Sekuritas Indonesia Tbk."},
    {"code": "MG", "name": "Semesta Indovest Sekuritas"},
    {"code": "MI", "name": "Victoria Sekuritas Indonesia"},
    {"code": "MU", "name": "Minna Padi Investama Sekuritas"},
    {"code": "NI", "name": "BNI Sekuritas"},
    {"code": "OD", "name": "BRI Danareksa Sekuritas"},
    {"code": "OK", "name": "NET SEKURITAS"},
    {"code": "PC", "name": "FAC Sekuritas Indonesia"},
    {"code": "PD", "name": "Indo Premier Sekuritas"},
    {"code": "PF", "name": "Danasakti Sekuritas Indonesia"},
    {"code": "PG", "name": "Panca Global Sekuritas"},
    {"code": "PI", "name": "Magenta Kapital Sekuritas Indonesia"},
    {"code": "PO", "name": "Pilarmas Investindo Sekuritas"},
    {"code": "PP", "name": "Aldiracita Sekuritas Indonesia"},
    {"code": "QA", "name": "Tuntun Sekuritas Indonesia"},
    {"code": "RB", "name": "Ina Sekuritas Indonesia"},
    {"code": "RF", "name": "Buana Capital Sekuritas"},
    {"code": "RG", "name": "Profindo Sekuritas Indonesia"},
    {"code": "RO", "name": "Pluang Maju Sekuritas"},
    {"code": "RS", "name": "Yulie Sekuritas Indonesia Tbk."},
    {"code": "RX", "name": "Macquarie Sekuritas Indonesia"},
    {"code": "SA", "name": "Elit Sukses Sekuritas"},
    {"code": "SF", "name": "Surya Fajar Sekuritas"},
    {"code": "SH", "name": "Artha Sekuritas Indonesia"},
    {"code": "SQ", "name": "BCA Sekuritas"},
    {"code": "SS", "name": "Supra Sekuritas Indonesia"},
    {"code": "TF", "name": "Laba Sekuritas Indonesia"},
    {"code": "TP", "name": "OCBC Sekuritas Indonesia"},
    {"code": "TS", "name": "Dwidana Sakti Sekuritas"},
    {"code": "XA", "name": "NH Korindo Sekuritas Indonesia"},
    {"code": "XC", "name": "Ajaib Sekuritas Asia"},
    {"code": "XL", "name": "Stockbit Sekuritas Digital"},
    {"code": "YB", "name": "Yakin Bertumbuh Sekuritas"},
    {"code": "YJ", "name": "Lotus Andalan Sekuritas"},
    {"code": "YO", "name": "Amantara Sekuritas Indonesia"},
    {"code": "YP", "name": "Mirae Asset Sekuritas Indonesia"},
    {"code": "YU", "name": "CGS International Sekuritas Indonesia"},
    {"code": "ZP", "name": "Maybank Sekuritas Indonesia"},
    {"code": "ZR", "name": "Bumiputera Sekuritas"},
]


@dataclass
class BrokerCrawlerConfig:
    """Configuration for the broker crawler."""
    base_url: str = "https://neobdm.tech"
    login_path: str = "/accounts/login/"
    broker_stalker_path: str = "/broker_stalker/"
    dash_callback_path: str = "/django_plotly_dash/app/bs_app/_dash-update-component"
    username: str = field(default_factory=lambda: os.getenv("NEOBDM_USERNAME", ""))
    password: str = field(default_factory=lambda: os.getenv("NEOBDM_PASSWORD", ""))
    output_dir: str = "output"
    checkpoint_file: str = ".crawler_checkpoint.json"
    rate_limit_seconds: float = 1.0
    # Retry configuration
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 10.0
    # Session configuration
    session_max_age_minutes: int = 25  # Refresh session before 30min expiry

    @property
    def login_url(self) -> str:
        return f"{self.base_url}{self.login_path}"

    @property
    def broker_stalker_url(self) -> str:
        return f"{self.base_url}{self.broker_stalker_path}"

    @property
    def dash_callback_url(self) -> str:
        return f"{self.base_url}{self.dash_callback_path}"


@dataclass
class BrokerDataRow:
    """A single row of broker trading data."""
    broker_code: str
    broker_name: str
    table_type: str  # "buy" or "sell"
    symbol: str
    netval: float  # in milyar Rp
    bval: float  # in milyar Rp
    sval: float  # in milyar Rp
    bavg: float  # average buying price
    savg: float  # average selling price
    crawl_date: str
    crawl_timestamp: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "broker_code": self.broker_code,
            "broker_name": self.broker_name,
            "table_type": self.table_type,
            "symbol": self.symbol,
            "netval": self.netval,
            "bval": self.bval,
            "sval": self.sval,
            "bavg": self.bavg,
            "savg": self.savg,
            "crawl_date": self.crawl_date,
            "crawl_timestamp": self.crawl_timestamp,
        }


class BrokerCrawler:
    """Crawls broker trading data from NeoBDM Broker Stalker."""

    def __init__(self, config: BrokerCrawlerConfig | None = None):
        """Initialize the crawler with optional configuration."""
        self.config = config or BrokerCrawlerConfig()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._logged_in = False
        self._csrf_token: str | None = None
        self._session_created_at: datetime | None = None

    def login(self) -> bool:
        """
        Authenticate with the NeoBDM website.
        
        Returns:
            True if login successful, False otherwise.
        """
        if not self.config.username or not self.config.password:
            logger.error("Username or password not provided. Set NEOBDM_USERNAME and NEOBDM_PASSWORD env vars.")
            return False

        try:
            # Step 1: Get the login page to obtain CSRF token
            logger.info("Fetching login page for CSRF token...")
            login_page = self.session.get(self.config.login_url)
            login_page.raise_for_status()

            # Extract CSRF token from the page
            soup = BeautifulSoup(login_page.text, "html.parser")
            csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
            if not csrf_input:
                logger.error("Could not find CSRF token on login page")
                return False

            csrf_token = csrf_input.get("value", "")
            self._csrf_token = csrf_token

            # Step 2: Submit login form
            logger.info(f"Logging in as {self.config.username}...")
            login_data = {
                "csrfmiddlewaretoken": csrf_token,
                "login": self.config.username,
                "password": self.config.password,
            }
            login_response = self.session.post(
                self.config.login_url,
                data=login_data,
                headers={
                    "Referer": self.config.login_url,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                allow_redirects=True,
            )
            login_response.raise_for_status()

            # Check if we're redirected to the broker stalker page (success)
            # or back to login page (failure)
            if "login" in login_response.url.lower() and "next" not in login_response.url.lower():
                logger.error("Login failed - still on login page")
                return False

            # Verify we can access the broker stalker page
            test_response = self.session.get(self.config.broker_stalker_url)
            if "login" in test_response.url.lower():
                logger.error("Login failed - redirected to login when accessing broker stalker")
                return False

            self._logged_in = True
            self._session_created_at = datetime.now()
            logger.info("Login successful!")
            
            # Initialize the Dash app
            if not self._initialize_dash_app():
                logger.warning("Dash app initialization may have failed, but continuing...")
            
            return True

        except requests.RequestException as e:
            logger.error(f"Login request failed: {e}")
            return False

    def _initialize_dash_app(self) -> bool:
        """
        Initialize the Dash app by loading its layout and dependencies.
        
        This is required before making callback requests to avoid 500 errors.
        """
        try:
            # Step 1: Load the broker stalker page to get the Dash app iframe/embed
            logger.debug("Initializing Dash app...")
            page_response = self.session.get(self.config.broker_stalker_url)
            page_response.raise_for_status()
            
            # Step 2: Load the Dash layout endpoint
            layout_url = f"{self.config.base_url}/django_plotly_dash/app/bs_app/_dash-layout"
            layout_response = self.session.get(
                layout_url,
                headers={
                    "Accept": "application/json",
                    "Referer": self.config.broker_stalker_url,
                }
            )
            layout_response.raise_for_status()
            
            # Step 3: Load the Dash dependencies
            deps_url = f"{self.config.base_url}/django_plotly_dash/app/bs_app/_dash-dependencies"
            deps_response = self.session.get(
                deps_url,
                headers={
                    "Accept": "application/json",
                    "Referer": self.config.broker_stalker_url,
                }
            )
            deps_response.raise_for_status()
            
            logger.debug("Dash app initialized successfully")
            return True
            
        except requests.RequestException as e:
            logger.warning(f"Dash app initialization failed: {e}")
            return False

    def _get_csrf_from_cookies(self) -> str:
        """Get CSRF token from cookies."""
        return self.session.cookies.get("csrftoken", "")

    def _is_session_expired(self) -> bool:
        """Check if the session might be expired based on age."""
        if not self._session_created_at:
            return True
        age_minutes = (datetime.now() - self._session_created_at).total_seconds() / 60
        return age_minutes >= self.config.session_max_age_minutes

    def _ensure_session_valid(self) -> bool:
        """
        Ensure the session is still valid. Refresh if needed.
        
        Returns:
            True if session is valid, False if refresh failed.
        """
        if not self._logged_in:
            return False
        
        if self._is_session_expired():
            logger.info("Session may be expiring, refreshing...")
            return self._refresh_session()
        
        return True

    def _refresh_session(self) -> bool:
        """
        Refresh the session by re-logging in.
        
        Returns:
            True if refresh successful, False otherwise.
        """
        logger.info("Refreshing session...")
        self._logged_in = False
        self._session_created_at = None
        
        # Clear existing session cookies
        self.session.cookies.clear()
        
        # Re-login
        return self.login()

    def _build_fetch_payload(self, broker_code: str, date_value: str = "Today") -> dict:
        """
        Build the Dash callback payload for fetching broker data.
        
        This mimics the POST request sent when clicking the 'Fetch' button.
        Based on actual network request captured from browser.
        """
        return {
            "output": "..broker-akum-stalker.children...broker-dist-stalker.children..",
            "outputs": [
                {"id": "broker-akum-stalker", "property": "children"},
                {"id": "broker-dist-stalker", "property": "children"},
            ],
            "inputs": [
                {"id": "submit-button", "property": "n_clicks", "value": 1},
                {"id": "duration-picker", "property": "value", "value": date_value},
            ],
            "changedPropIds": ["submit-button.n_clicks"],
            "parsedChangedPropsIds": ["submit-button.n_clicks"],
            "state": [
                {"id": "broker", "property": "value", "value": [broker_code]},
            ],
        }

    def _send_dash_request(self, payload: dict, retry_on_auth_fail: bool = True) -> dict | None:
        """
        Send a POST request to the Dash callback endpoint with retry logic.
        
        Args:
            payload: The JSON payload for the Dash callback
            retry_on_auth_fail: Whether to retry after session refresh on auth failure
        
        Returns:
            The JSON response or None if all retries failed.
        """
        csrf_token = self._get_csrf_from_cookies()
        last_error: Exception | None = None
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.post(
                    self.config.dash_callback_url,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "X-CSRFToken": csrf_token,
                        "Referer": self.config.broker_stalker_url,
                    },
                    timeout=30,
                )
                
                # Check for auth errors
                if response.status_code in [401, 403]:
                    if retry_on_auth_fail:
                        logger.warning(f"Auth error (attempt {attempt + 1}), refreshing session...")
                        if self._refresh_session():
                            csrf_token = self._get_csrf_from_cookies()
                            continue  # Retry with new session
                    logger.error("Authentication failed after session refresh")
                    return None
                
                # Check for server errors (worth retrying)
                if response.status_code >= 500:
                    delay = min(
                        self.config.retry_base_delay * (2 ** attempt),
                        self.config.retry_max_delay
                    )
                    logger.warning(f"Server error {response.status_code} (attempt {attempt + 1}/{self.config.max_retries}), retrying in {delay:.1f}s...")
                    time.sleep(delay)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout as e:
                delay = min(
                    self.config.retry_base_delay * (2 ** attempt),
                    self.config.retry_max_delay
                )
                logger.warning(f"Timeout (attempt {attempt + 1}/{self.config.max_retries}), retrying in {delay:.1f}s...")
                last_error = e
                time.sleep(delay)
                
            except requests.exceptions.ConnectionError as e:
                delay = min(
                    self.config.retry_base_delay * (2 ** attempt),
                    self.config.retry_max_delay
                )
                logger.warning(f"Connection error (attempt {attempt + 1}/{self.config.max_retries}), retrying in {delay:.1f}s...")
                last_error = e
                time.sleep(delay)
                
            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                last_error = e
                break  # Don't retry other request errors
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                return None
        
        if last_error:
            logger.error(f"All {self.config.max_retries} retries exhausted. Last error: {last_error}")
        return None

    def _parse_table_data(
        self,
        data: list[dict],
        broker_code: str,
        broker_name: str,
        table_type: str,
        crawl_date: str,
        crawl_timestamp: str,
    ) -> list[BrokerDataRow]:
        """Parse table data from Dash response into BrokerDataRow objects."""
        rows = []
        for item in data:
            try:
                # Extract symbol from markdown link format: [BBNI](/stock_detail/BBNI)
                symbol_raw = str(item.get("symbol", ""))
                symbol_match = re.match(r'\[([A-Z0-9-]+)\]', symbol_raw)
                symbol = symbol_match.group(1) if symbol_match else symbol_raw
                
                row = BrokerDataRow(
                    broker_code=broker_code,
                    broker_name=broker_name,
                    table_type=table_type,
                    symbol=symbol,
                    netval=float(item.get("netval", 0)),
                    bval=float(item.get("bval", 0)),
                    sval=float(item.get("sval", 0)),
                    bavg=float(item.get("bavg", 0)),
                    savg=float(item.get("savg", 0)),
                    crawl_date=crawl_date,
                    crawl_timestamp=crawl_timestamp,
                )
                rows.append(row)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse row: {item}, error: {e}")
                continue
        return rows

    def fetch_broker_data(
        self, 
        broker_code: str, 
        broker_name: str = "",
        date_value: str = "Today"
    ) -> list[BrokerDataRow]:
        """
        Fetch all trading data for a single broker.
        
        Args:
            broker_code: The broker code (e.g., "AD")
            broker_name: The broker's company name
            date_value: Date value (default "Today")
            
        Returns:
            List of BrokerDataRow objects containing buy and sell data.
        """
        if not self._logged_in:
            logger.error("Not logged in. Call login() first.")
            return []

        crawl_date = datetime.now().strftime("%Y-%m-%d")
        crawl_timestamp = datetime.now().isoformat()
        all_rows: list[BrokerDataRow] = []

        logger.info(f"Fetching data for broker {broker_code} ({broker_name})...")

        # Fetch data from API
        payload = self._build_fetch_payload(broker_code, date_value)
        response = self._send_dash_request(payload)

        if not response or "response" not in response:
            logger.warning(f"No data returned for broker {broker_code}")
            return []

        resp_data = response.get("response", {})

        # Parse buy table data from broker-akum-stalker
        akum_data = resp_data.get("broker-akum-stalker", {})
        buy_data = self._extract_table_data_from_children(akum_data)
        if buy_data:
            rows = self._parse_table_data(
                buy_data, broker_code, broker_name, "buy", crawl_date, crawl_timestamp
            )
            all_rows.extend(rows)
            logger.debug(f"  Buy table: {len(rows)} rows")

        # Parse sell table data from broker-dist-stalker
        dist_data = resp_data.get("broker-dist-stalker", {})
        sell_data = self._extract_table_data_from_children(dist_data)
        if sell_data:
            rows = self._parse_table_data(
                sell_data, broker_code, broker_name, "sell", crawl_date, crawl_timestamp
            )
            all_rows.extend(rows)
            logger.debug(f"  Sell table: {len(rows)} rows")

        logger.info(f"  Broker {broker_code}: {len(all_rows)} total rows")
        return all_rows

    def _extract_table_data_from_children(self, component: dict) -> list[dict] | None:
        """
        Extract table data from a Dash component's children.
        
        The response structure is:
        {
            "children": [
                {"type": "Label", "props": {"children": "label text"}},
                {"type": "DataTable", "props": {"data": [...]}}
            ]
        }
        """
        if not component or "children" not in component:
            return None
        
        children = component.get("children", [])
        for child in children:
            if isinstance(child, dict):
                child_type = child.get("type", "")
                if child_type == "DataTable":
                    props = child.get("props", {})
                    return props.get("data", [])
        
        return None

    def _get_checkpoint_path(self) -> Path:
        """Get the path to the checkpoint file."""
        return Path(self.config.output_dir) / self.config.checkpoint_file

    def _load_checkpoint(self) -> dict | None:
        """
        Load checkpoint from file if it exists and is recent.
        
        Returns:
            Checkpoint data or None if no valid checkpoint exists.
        """
        checkpoint_path = self._get_checkpoint_path()
        if not checkpoint_path.exists():
            return None
        
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                checkpoint = json.load(f)
            
            # Check if checkpoint is recent (within 2 hours)
            started_at = datetime.fromisoformat(checkpoint.get("started_at", ""))
            age_hours = (datetime.now() - started_at).total_seconds() / 3600
            
            if age_hours > 2:
                logger.info("Checkpoint is too old (>2 hours), starting fresh")
                return None
            
            logger.info(f"Found checkpoint from {started_at.strftime('%H:%M:%S')}, "
                       f"last broker: {checkpoint.get('last_broker_code')}")
            return checkpoint
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.warning(f"Failed to load checkpoint: {e}")
            return None

    def _save_checkpoint(
        self, 
        started_at: datetime | str,
        broker_index: int,
        broker_code: str,
        completed_brokers: list[str],
        failed_brokers: list[str],
        partial_data: list[dict],
    ) -> None:
        """Save current progress to checkpoint file."""
        checkpoint_path = self._get_checkpoint_path()
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Handle both datetime and string for started_at
        started_at_str = started_at.isoformat() if isinstance(started_at, datetime) else started_at
        
        checkpoint = {
            "started_at": started_at_str,
            "last_broker_index": broker_index,
            "last_broker_code": broker_code,
            "completed_brokers": completed_brokers,
            "failed_brokers": failed_brokers,
            "partial_data": partial_data,
        }
        
        with open(checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2)

    def _clear_checkpoint(self) -> None:
        """Remove the checkpoint file after successful completion."""
        checkpoint_path = self._get_checkpoint_path()
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            logger.debug("Checkpoint file cleared")

    def crawl_all_brokers(
        self, 
        broker_codes: list[dict[str, str]] | None = None,
        date_value: str = "Today",
        resume: bool = False,
    ) -> dict[str, Any]:
        """
        Crawl data for all brokers with optional resume support.
        
        Args:
            broker_codes: List of broker dicts with 'code' and 'name' keys.
                         Defaults to BROKER_CODES.
            date_value: Date value (default "Today")
            resume: Whether to resume from checkpoint if available.
            
        Returns:
            Dictionary with crawl results and metadata.
        """
        if not self._logged_in:
            logger.error("Not logged in. Call login() first.")
            return {"error": "Not logged in"}

        brokers = broker_codes or BROKER_CODES
        all_data: list[dict] = []
        failed_brokers: list[str] = []
        successful_brokers: list[str] = []
        start_index = 0

        # Check for existing checkpoint if resume is enabled
        checkpoint = None
        if resume:
            checkpoint = self._load_checkpoint()
            if checkpoint:
                start_index = checkpoint.get("last_broker_index", 0) + 1
                successful_brokers = checkpoint.get("completed_brokers", [])
                failed_brokers = checkpoint.get("failed_brokers", [])
                all_data = checkpoint.get("partial_data", [])
                logger.info(f"Resuming from broker #{start_index + 1} ({len(all_data)} rows already collected)")

        start_time = datetime.now()
        
        if start_index == 0:
            logger.info(f"Starting crawl for {len(brokers)} brokers...")
        else:
            logger.info(f"Continuing crawl for remaining {len(brokers) - start_index} brokers...")

        for i, broker in enumerate(brokers[start_index:], start_index + 1):
            code = broker["code"]
            name = broker["name"]
            
            # Check and refresh session if needed
            self._ensure_session_valid()
            
            logger.info(f"[{i}/{len(brokers)}] Processing broker {code}...")
            
            try:
                rows = self.fetch_broker_data(code, name, date_value)
                if rows:
                    all_data.extend([row.to_dict() for row in rows])
                    successful_brokers.append(code)
                else:
                    logger.warning(f"No data for broker {code}")
                    failed_brokers.append(code)
            except Exception as e:
                logger.error(f"Error processing broker {code}: {e}")
                failed_brokers.append(code)

            # Save checkpoint after each broker
            self._save_checkpoint(
                started_at=checkpoint.get("started_at", start_time.isoformat()) if checkpoint else start_time,
                broker_index=i - 1,  # 0-indexed
                broker_code=code,
                completed_brokers=successful_brokers,
                failed_brokers=failed_brokers,
                partial_data=all_data,
            )

            # Rate limiting between brokers
            if i < len(brokers):
                time.sleep(self.config.rate_limit_seconds)

        end_time = datetime.now()
        
        # Calculate total duration (including resumed time)
        if checkpoint:
            original_start = datetime.fromisoformat(checkpoint.get("started_at", start_time.isoformat()))
            duration = (end_time - original_start).total_seconds()
        else:
            duration = (end_time - start_time).total_seconds()

        result = {
            "metadata": {
                "crawl_date": start_time.strftime("%Y-%m-%d"),
                "crawl_start": start_time.isoformat(),
                "crawl_end": end_time.isoformat(),
                "duration_seconds": duration,
                "total_brokers": len(brokers),
                "successful_brokers": len(successful_brokers),
                "failed_brokers": len(failed_brokers),
                "total_rows": len(all_data),
                "resumed": resume and checkpoint is not None,
            },
            "successful_broker_codes": successful_brokers,
            "failed_broker_codes": failed_brokers,
            "data": all_data,
        }

        # Clear checkpoint on successful completion
        self._clear_checkpoint()

        logger.info(f"Crawl completed in {duration:.1f}s. "
                   f"Success: {len(successful_brokers)}, Failed: {len(failed_brokers)}, "
                   f"Total rows: {len(all_data)}")

        return result

    def save_to_json(self, data: dict[str, Any], filename: str | None = None) -> str:
        """
        Save crawl results to a JSON file.
        
        Args:
            data: The crawl results dictionary
            filename: Optional custom filename
            
        Returns:
            The path to the saved file.
        """
        # Create output directory if it doesn't exist
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename with date
        if not filename:
            date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"broker_data_{date_str}.json"

        filepath = output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Data saved to {filepath}")
        return str(filepath)


def test_crawler_access() -> bool:
    """
    Test that the crawler can authenticate and access different brokers.
    
    Returns:
        True if all tests pass, False otherwise.
    """
    logger.info("=" * 50)
    logger.info("Running crawler access tests...")
    logger.info("=" * 50)

    config = BrokerCrawlerConfig()
    crawler = BrokerCrawler(config)

    # Test 1: Login
    logger.info("\nTest 1: Authentication")
    if not crawler.login():
        logger.error("FAILED: Could not authenticate")
        return False
    logger.info("PASSED: Authentication successful")

    # Test 2: Fetch data for first broker (AD)
    logger.info("\nTest 2: Fetch data for broker AD")
    rows = crawler.fetch_broker_data("AD", "OSO Sekuritas Indonesia")
    if not rows:
        logger.error("FAILED: No data returned for broker AD")
        return False
    logger.info(f"PASSED: Got {len(rows)} rows for broker AD")

    # Test 3: Fetch data for a middle broker (KK)
    logger.info("\nTest 3: Fetch data for broker KK")
    time.sleep(1)  # Rate limit
    rows = crawler.fetch_broker_data("KK", "Phillip Sekuritas Indonesia")
    if not rows:
        logger.error("FAILED: No data returned for broker KK")
        return False
    logger.info(f"PASSED: Got {len(rows)} rows for broker KK")

    # Test 4: Fetch data for last broker (ZR)
    logger.info("\nTest 4: Fetch data for broker ZR")
    time.sleep(1)  # Rate limit
    rows = crawler.fetch_broker_data("ZR", "Bumiputera Sekuritas")
    if not rows:
        logger.warning("WARNING: No data returned for broker ZR (might be normal if no trades)")
    else:
        logger.info(f"PASSED: Got {len(rows)} rows for broker ZR")

    logger.info("\n" + "=" * 50)
    logger.info("All tests passed!")
    logger.info("=" * 50)
    return True


def main():
    """Main entry point for the crawler."""
    import argparse

    parser = argparse.ArgumentParser(description="Crawl broker data from NeoBDM")
    parser.add_argument("--test", action="store_true", help="Run access tests only")
    parser.add_argument("--broker", type=str, help="Crawl specific broker code only")
    parser.add_argument("--output", type=str, help="Custom output filename")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint if available")
    parser.add_argument("--fresh", action="store_true", help="Ignore checkpoint and start fresh")
    args = parser.parse_args()

    if args.test:
        success = test_crawler_access()
        exit(0 if success else 1)

    config = BrokerCrawlerConfig()
    crawler = BrokerCrawler(config)

    # Clear checkpoint if --fresh flag is used
    if args.fresh:
        crawler._clear_checkpoint()
        logger.info("Checkpoint cleared, starting fresh crawl")

    # Login
    if not crawler.login():
        logger.error("Failed to login. Check your credentials.")
        exit(1)

    # Crawl data
    if args.broker:
        # Find broker name
        broker_info = next(
            (b for b in BROKER_CODES if b["code"] == args.broker.upper()), 
            None
        )
        if not broker_info:
            logger.error(f"Unknown broker code: {args.broker}")
            exit(1)
        
        rows = crawler.fetch_broker_data(broker_info["code"], broker_info["name"])
        data = {
            "metadata": {
                "crawl_date": datetime.now().strftime("%Y-%m-%d"),
                "broker_code": broker_info["code"],
                "broker_name": broker_info["name"],
                "total_rows": len(rows),
            },
            "data": [row.to_dict() for row in rows],
        }
    else:
        # Crawl all brokers (with resume support)
        data = crawler.crawl_all_brokers(resume=args.resume)

    # Save results
    filepath = crawler.save_to_json(data, args.output)
    print(f"\nData saved to: {filepath}")


if __name__ == "__main__":
    main()
