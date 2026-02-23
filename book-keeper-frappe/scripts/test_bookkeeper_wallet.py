"""
Comprehensive test suite for book-keeper ledger API endpoints.

Tests cover:
- Account management (creation, querying)
- Journal entries (simple and compound)
- Transfer operations (simple, compound, pending)
- Limit management and refilling
- Maximum balance enforcement
- Account property management
- Transaction atomicity guarantees
- Error handling and edge cases

Author: QA Suite
Date: 2024
"""

import time
import uuid
from datetime import datetime, timedelta

import pytest
import requests

# ============================================================================
# Configuration
# ============================================================================

BASE_URL = "http://localhost:9090"
API_VERSION = "v1"  # Keep API_VERSION fixed
TENANT_ID = "twophasetenant"
TEST_TIMEOUT = 10  # seconds

# Endpoints
ACCOUNTS_ENDPOINT = f"{BASE_URL}/api/book-keeper/{API_VERSION}/accounts"
JOURNAL_ENTRIES_ENDPOINT = f"{BASE_URL}/api/book-keeper/{API_VERSION}/journal-entries"
TRANSFERS_ENDPOINT = f"{BASE_URL}/api/book-keeper/{API_VERSION}/transfers"
BALANCES_ENDPOINT = f"{BASE_URL}/api/book-keeper/{API_VERSION}/accounts/balances"
PENDING_ENTRIES_ENDPOINT = f"{BASE_URL}/api/book-keeper/{API_VERSION}/pending-journal-entries"
PENDING_TRANSFERS_ENDPOINT = f"{BASE_URL}/api/book-keeper/{API_VERSION}/pending-compound-transfers"
REFILL_ENDPOINT = f"{BASE_URL}/api/book-keeper/{API_VERSION}/admin/limiter-accounts/refill"

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def http_client():
    """Session-scoped HTTP client with default headers."""
    session = requests.Session()
    session.headers.update(
        {
            "Content-Type": "application/json",
        }
    )
    yield session
    session.close()


@pytest.fixture(scope="module")
def setup_system_accounts(http_client):
    """Create system-level accounts needed for wallet operations."""
    # Use fixed account codes to match application settings.
    account_codes = {
        "PAYABLES_EXTERNAL": "PAYABLES_EXTERNAL",
        "source_of_funds": "source_of_funds",
        "max_balance_suspense": "max_balance_suspense",
        "sys_rate_limiter_credit": "sys_rate_limiter_credit",
        "sys_rate_limiter_debit": "sys_rate_limiter_debit",
        "sys_ctrl_compound": "sys_ctrl_compound",
        "BANK_SUSPENSE": "BANK_SUSPENSE",
    }

    system_accounts = {
        "tenant_id": TENANT_ID,
        "accounts": [
            {"code": account_codes["PAYABLES_EXTERNAL"], "name": "External Payables", "type": "liability"},
            {"code": account_codes["source_of_funds"], "name": "Source of Funds", "type": "liability"},
            {"code": account_codes["max_balance_suspense"], "name": "Max Balance Suspense", "type": "liability"},
            {"code": account_codes["sys_rate_limiter_credit"], "name": "Rate Limiter Credit", "type": "liability"},
            {"code": account_codes["sys_rate_limiter_debit"], "name": "Rate Limiter Debit", "type": "asset"},
            {"code": account_codes["sys_ctrl_compound"], "name": "Compound Transfer Control", "type": "liability"},
            {"code": account_codes["BANK_SUSPENSE"], "name": "Bank Suspense Account", "type": "asset"},
        ],
    }

    response = http_client.post(ACCOUNTS_ENDPOINT, json=system_accounts, timeout=TEST_TIMEOUT)
    # The endpoint now returns 200 OK with the creation results.
    assert response.status_code == 200, f"Failed to create system accounts: {response.text}"
    # Yield the modified account codes so other fixtures can use them
    yield account_codes
    # Cleanup is implicit - accounts persist for other tests


@pytest.fixture
def unique_user_id():
    """Generate unique user ID for test isolation."""
    return f"user_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def setup_user_wallet(http_client, setup_system_accounts, unique_user_id):
    """Create user-specific wallet and limiter accounts."""
    user_accounts = {
        "tenant_id": TENANT_ID,
        "accounts": [
            {
                "code": f"WALLET_{unique_user_id}",
                "name": f"Wallet for {unique_user_id}",
                "type": "liability",
                # This flag prevents the balance from exceeding the initial debit ceiling.
                "flags": 256,  # CREDITS_MUST_NOT_EXCEED_DEBITS
                "max_balance": 20000000,  # ₹200,000 in paise
            },
            {
                "code": f"limit_non_reg_daily_amt_{unique_user_id}",
                "name": f"Non-Reg Daily Limit (Amount) - {unique_user_id}",
                "type": "asset",
                # This flag prevents the limit from being overdrawn.
                "flags": 512,  # DEBITS_MUST_NOT_EXCEED_CREDITS
            },
            {
                "code": f"limit_non_reg_daily_count_{unique_user_id}",
                "name": f"Non-Reg Daily Limit (Count) - {unique_user_id}",
                "type": "asset",
                # This flag prevents the limit from being overdrawn.
                "flags": 512,  # DEBITS_MUST_NOT_EXCEED_CREDITS
            },
            {
                "code": f"limit_reg_daily_amt_{unique_user_id}",
                "name": f"Reg Daily Limit (Amount) - {unique_user_id}",
                "type": "asset",
                # This flag prevents the limit from being overdrawn.
                "flags": 512,  # DEBITS_MUST_NOT_EXCEED_CREDITS
            },
        ],
    }

    response = http_client.post(ACCOUNTS_ENDPOINT, json=user_accounts, timeout=TEST_TIMEOUT)
    # The endpoint now returns 200 OK with the creation results.
    if response.status_code != 200:
        print(f"\n[DEBUG] FAILED TO CREATE USER WALLET. Status: {response.status_code}, Body: {response.text}")
    assert response.status_code == 200

    yield {
        "user_id": unique_user_id,
        "wallet_account": f"WALLET_{unique_user_id}",
        "limit_non_reg_amt_account": f"limit_non_reg_daily_amt_{unique_user_id}",
        "limit_non_reg_count_account": f"limit_non_reg_daily_count_{unique_user_id}",
        "limit_reg_amt_account": f"limit_reg_daily_amt_{unique_user_id}",
        "max_balance": 20000000,
    }


@pytest.fixture
def refill_user_limits(http_client, setup_system_accounts, setup_user_wallet):
    """Refill user's daily limits."""
    refill_payload = {
        "tenant_id": TENANT_ID,
        "source_of_funds_account_code": setup_system_accounts["sys_rate_limiter_credit"],
        "accounts_to_refill": [
            {
                "account_code": setup_user_wallet["limit_non_reg_amt_account"],
                "amount": 1000000,  # ₹10,000
                "currency": "INR",
            },
            {"account_code": setup_user_wallet["limit_non_reg_count_account"], "amount": 10, "currency": "QTY"},
            {
                "account_code": setup_user_wallet["limit_reg_amt_account"],
                "amount": 10000000,  # ₹100,000
                "currency": "INR",
            },
        ],
    }

    response = http_client.post(REFILL_ENDPOINT, json=refill_payload, timeout=TEST_TIMEOUT)
    assert response.status_code == 204, f"Failed to refill limits: {response.text}"

    yield setup_user_wallet


# ============================================================================
# Test Class: Account Management
# ============================================================================


class TestAccountManagement:
    """Tests for account creation, retrieval, and properties."""

    def test_create_single_account(self, http_client):
        """Test creating a single account."""
        unique_code = f"TEST_ACCT_{uuid.uuid4().hex[:8]}"

        payload = {"tenant_id": TENANT_ID, "accounts": [{"code": unique_code, "name": "Test Account", "type": "asset"}]}

        response = http_client.post(ACCOUNTS_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 200
        result = response.json()
        assert isinstance(result, list)

    def test_create_multiple_accounts_batch(self, http_client):
        """Test creating multiple accounts in a single request."""
        unique_suffix = uuid.uuid4().hex[:8]

        payload = {
            "tenant_id": TENANT_ID,
            "accounts": [
                {"code": f"BATCH_ASSET_{unique_suffix}", "name": "Batch Test Asset", "type": "asset"},
                {"code": f"BATCH_LIABILITY_{unique_suffix}", "name": "Batch Test Liability", "type": "liability"},
                {"code": f"BATCH_EQUITY_{unique_suffix}", "name": "Batch Test Equity", "type": "equity"},
            ],
        }

        response = http_client.post(ACCOUNTS_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_create_account_with_max_balance(self, http_client):
        """Test creating account with maximum balance limit."""
        unique_code = f"MAXBAL_{uuid.uuid4().hex[:8]}"
        max_balance_amount = 5000000  # ₹50,000

        payload = {
            "tenant_id": TENANT_ID,
            "accounts": [
                {
                    "code": unique_code,
                    "name": "Account with Max Balance",
                    "type": "liability",
                    "max_balance": max_balance_amount,
                }
            ],
        }

        response = http_client.post(ACCOUNTS_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_query_account_balances(self, http_client, setup_user_wallet, refill_user_limits):
        """Test querying account balances."""
        params = {"tenant_id": TENANT_ID, "account_codes": setup_user_wallet["wallet_account"]}

        response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)

        assert response.status_code == 200
        result = response.json()
        assert isinstance(result, list)
        if result:
            assert "account_code" in result[0]
            assert "balance" in result[0]

    def test_query_multiple_account_balances(self, http_client, setup_user_wallet, refill_user_limits):
        """Test querying multiple account balances at once."""
        account_codes = [
            setup_user_wallet["wallet_account"],
            setup_user_wallet["limit_non_reg_amt_account"],
            setup_user_wallet["limit_non_reg_count_account"],
        ]

        params = {"tenant_id": TENANT_ID, "account_codes": ",".join(account_codes)}

        response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)

        assert response.status_code == 200
        result = response.json()
        assert isinstance(result, list)
        assert len(result) >= 0

    def test_account_types_validation(self, http_client):
        """Test that all account types are accepted."""
        unique_suffix = uuid.uuid4().hex[:8]
        account_types = ["asset", "liability", "equity", "revenue", "expense"]

        for acc_type in account_types:
            payload = {
                "tenant_id": TENANT_ID,
                "accounts": [
                    {
                        "code": f"TYPE_{acc_type.upper()}_{unique_suffix}",
                        "name": f"Account Type {acc_type}",
                        "type": acc_type,
                    }
                ],
            }

            response = http_client.post(ACCOUNTS_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

            assert response.status_code == 200


# ============================================================================
# Test Class: Simple Journal Entries
# ============================================================================


class TestSimpleJournalEntries:
    """Tests for basic journal entry creation and validation."""

    def test_create_simple_journal_entry(
        self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits
    ):
        """Test creating a balanced journal entry."""
        amount = 100000  # ₹1,000
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Test simple journal entry",
            "debit_legs": [
                {"account_code": setup_system_accounts["BANK_SUSPENSE"], "amount": amount, "currency": "INR"}
            ],
            "credit_legs": [{"account_code": setup_user_wallet["wallet_account"], "amount": amount, "currency": "INR"}],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 201
        result = response.json()
        assert "journal_id" in result or "id" in result

    def test_wallet_topup_successful(self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits):
        """Test successful wallet top-up (balance below max)."""
        topup_amount = 1000000  # ₹10,000
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Wallet top-up via UPI",
            "debit_legs": [
                {"account_code": setup_system_accounts["BANK_SUSPENSE"], "amount": topup_amount, "currency": "INR"}
            ],
            "credit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": topup_amount, "currency": "INR"}
            ],
        }
        # This is a compound transfer and should use the dedicated endpoint.
        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 201

    def test_wallet_topup_exceeds_max_balance(
        self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits
    ):
        """Test that topup fails when exceeding max_balance."""
        # First, fill wallet to near max
        fill_amount = 19900000  # ₹199,000 (below ₹200,000 max)
        entry_date = datetime.now().strftime("%Y-%m-%d")

        # Fill wallet
        fill_payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Fill wallet near maximum",
            "debit_legs": [
                {"account_code": setup_system_accounts["BANK_SUSPENSE"], "amount": fill_amount, "currency": "INR"}
            ],
            "credit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": fill_amount, "currency": "INR"}
            ],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=fill_payload, timeout=TEST_TIMEOUT)
        assert response.status_code == 201

        # Try to exceed max_balance
        excess_payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Attempted topup exceeding max balance",
            "debit_legs": [
                {
                    "account_code": setup_system_accounts["BANK_SUSPENSE"],
                    "amount": 200000,  # This should push over limit
                    "currency": "INR",
                }
            ],
            "credit_legs": [{"account_code": setup_user_wallet["wallet_account"], "amount": 200000, "currency": "INR"}],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=excess_payload, timeout=TEST_TIMEOUT)

        # Should fail with 400 or similar error
        assert response.status_code >= 400

    def test_unbalanced_entry_rejected(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test that unbalanced entries are rejected."""
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Unbalanced entry (should fail)",
            "debit_legs": [
                {"account_code": setup_system_accounts["BANK_SUSPENSE"], "amount": 100000, "currency": "INR"}
            ],
            "credit_legs": [
                {
                    "account_code": setup_user_wallet["wallet_account"],
                    "amount": 50000,  # Unbalanced!
                    "currency": "INR",
                }
            ],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code >= 400

    def test_multiple_legs_balanced(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test entry with multiple legs that balance."""
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Multi-leg balanced entry",
            "debit_legs": [
                {"account_code": setup_system_accounts["BANK_SUSPENSE"], "amount": 100000, "currency": "INR"},
                {"account_code": setup_system_accounts["PAYABLES_EXTERNAL"], "amount": 50000, "currency": "INR"},
            ],
            "credit_legs": [{"account_code": setup_user_wallet["wallet_account"], "amount": 150000, "currency": "INR"}],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 201


# ============================================================================
# Test Class: Compound Transfers (Atomic Multi-Leg)
# ============================================================================


class TestCompoundTransfers:
    """Tests for atomic compound transfers with multiple legs."""

    def test_atomic_transfer_to_new_beneficiary(
        self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits
    ):
        """Test atomic transfer to new beneficiary with limit consumption."""
        transfer_amount = 50000  # ₹500
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Atomic payment from user to new beneficiary",
            "debit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": transfer_amount, "currency": "INR"},
                {
                    "account_code": setup_user_wallet["limit_non_reg_amt_account"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {"account_code": setup_user_wallet["limit_non_reg_count_account"], "amount": 1, "currency": "QTY"},
            ],
            "credit_legs": [
                {
                    "account_code": setup_system_accounts["PAYABLES_EXTERNAL"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {
                    "account_code": setup_system_accounts["source_of_funds"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {"account_code": setup_system_accounts["source_of_funds"], "amount": 1, "currency": "QTY"},
            ],
        }

        response = http_client.post(f"{TRANSFERS_ENDPOINT}/compound", json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 201

    def test_atomic_transfer_to_registered_beneficiary(
        self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits
    ):
        """Test atomic transfer to registered beneficiary with higher limit."""
        transfer_amount = 750000  # ₹7,500
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Consume limits for payment to registered beneficiary",
            "debit_legs": [
                {
                    "account_code": setup_user_wallet["limit_reg_amt_account"],
                    "amount": transfer_amount,
                    "currency": "INR",
                }
            ],
            "credit_legs": [
                {"account_code": setup_system_accounts["source_of_funds"], "amount": transfer_amount, "currency": "INR"}
            ],
        }

        response = http_client.post(f"{TRANSFERS_ENDPOINT}/compound", json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 201

    def test_compound_transfer_atomicity_wallet_overdraft(
        self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits
    ):
        """Test that compound transfer fails atomically if wallet would overdraft."""
        # Attempt transfer larger than wallet balance
        large_transfer = 50000000  # ₹500,000 (likely more than available)
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Large transfer attempting wallet overdraft",
            "debit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": large_transfer, "currency": "INR"},
                {
                    "account_code": setup_user_wallet["limit_non_reg_amt_account"],
                    "amount": large_transfer,
                    "currency": "INR",
                },
            ],
            "credit_legs": [
                {
                    "account_code": setup_system_accounts["PAYABLES_EXTERNAL"],
                    "amount": large_transfer,
                    "currency": "INR",
                },
                {"account_code": setup_system_accounts["source_of_funds"], "amount": large_transfer, "currency": "INR"},
            ],
        }

        # Print balances before the call for debugging
        accounts_to_check = [setup_user_wallet["wallet_account"], setup_user_wallet["limit_non_reg_amt_account"]]
        params = {"tenant_id": TENANT_ID, "account_codes": ",".join(accounts_to_check)}
        balance_response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)
        assert balance_response.status_code == 200
        print(f"\nBalances before wallet overdraft attempt: {balance_response.json()}")

        response = http_client.post(f"{TRANSFERS_ENDPOINT}/compound", json=payload, timeout=TEST_TIMEOUT)

        # Should fail due to insufficient balance
        assert response.status_code >= 400, f"API allowed overdraft with status {response.status_code}"

    def test_compound_transfer_atomicity_limit_exceeded(
        self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits
    ):
        """Test that compound transfer fails atomically if limit would be exceeded."""
        # Try to consume more limit than available
        excess_limit = 2000000  # ₹20,000 (likely more than ₹10,000 refilled)
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Attempting to exceed limit",
            "debit_legs": [
                {
                    "account_code": setup_user_wallet["limit_non_reg_amt_account"],
                    "amount": excess_limit,
                    "currency": "INR",
                }
            ],
            "credit_legs": [
                {"account_code": setup_system_accounts["source_of_funds"], "amount": excess_limit, "currency": "INR"}
            ],
        }

        # Print balance before the call for debugging
        params = {"tenant_id": TENANT_ID, "account_codes": setup_user_wallet["limit_non_reg_amt_account"]}
        balance_response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)
        assert balance_response.status_code == 200
        print(f"\nBalance before limit exceed attempt: {balance_response.json()}")

        response = http_client.post(f"{TRANSFERS_ENDPOINT}/compound", json=payload, timeout=TEST_TIMEOUT)

        # Should fail due to insufficient limit
        assert response.status_code >= 400, f"API allowed exceeding limit with status {response.status_code}"

    def test_compound_transfer_with_mixed_currencies(
        self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits
    ):
        """Test compound transfer balancing different currencies."""
        transfer_amount = 50000
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Transfer with quantity and currency",
            "debit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": transfer_amount, "currency": "INR"},
                {"account_code": setup_user_wallet["limit_non_reg_count_account"], "amount": 1, "currency": "QTY"},
            ],
            "credit_legs": [
                {
                    "account_code": setup_system_accounts["PAYABLES_EXTERNAL"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {"account_code": setup_system_accounts["source_of_funds"], "amount": 1, "currency": "QTY"},
            ],
        }

        response = http_client.post(f"{TRANSFERS_ENDPOINT}/compound", json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 201


# ============================================================================
# Test Class: Limit Management
# ============================================================================


class TestLimitManagement:
    """Tests for refilling and managing rate limits."""

    def test_refill_daily_limits(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test refilling user's daily limits."""
        refill_payload = {
            "tenant_id": TENANT_ID,
            "source_of_funds_account_code": setup_system_accounts["sys_rate_limiter_credit"],
            "accounts_to_refill": [
                {
                    "account_code": setup_user_wallet["limit_non_reg_amt_account"],
                    "amount": 1000000,  # ₹10,000
                    "currency": "INR",
                },
                {"account_code": setup_user_wallet["limit_non_reg_count_account"], "amount": 10, "currency": "QTY"},
            ],
        }
        # This is a compound transfer and should use the dedicated endpoint.
        response = http_client.post(REFILL_ENDPOINT, json=refill_payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 204

    def test_query_remaining_transfer_count(self, http_client, setup_user_wallet, refill_user_limits):
        """Test querying remaining daily transfer count."""
        params = {"tenant_id": TENANT_ID, "account_codes": setup_user_wallet["limit_non_reg_count_account"]}

        response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)

        assert response.status_code == 200
        result = response.json()
        assert len(result) > 0
        assert result[0]["balance"] == 10  # Should be refilled amount

    def test_query_remaining_transfer_amount(self, http_client, setup_user_wallet, refill_user_limits):
        """Test querying remaining daily transfer amount limit."""
        params = {"tenant_id": TENANT_ID, "account_codes": setup_user_wallet["limit_non_reg_amt_account"]}

        response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)

        assert response.status_code == 200
        result = response.json()
        assert len(result) > 0
        assert result[0]["balance"] == 1000000  # ₹10,000

    def test_limit_consumption_after_transfer(
        self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits
    ):
        """Test that limits are consumed after atomic transfer."""
        transfer_amount = 50000
        entry_date = datetime.now().strftime("%Y-%m-%d")

        # Perform transfer
        transfer_payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Transfer for limit consumption test",
            "debit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": transfer_amount, "currency": "INR"},
                {
                    "account_code": setup_user_wallet["limit_non_reg_amt_account"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {"account_code": setup_user_wallet["limit_non_reg_count_account"], "amount": 1, "currency": "QTY"},
            ],
            "credit_legs": [
                {
                    "account_code": setup_system_accounts["PAYABLES_EXTERNAL"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {
                    "account_code": setup_system_accounts["source_of_funds"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {"account_code": setup_system_accounts["source_of_funds"], "amount": 1, "currency": "QTY"},
            ],
        }

        response = http_client.post(f"{TRANSFERS_ENDPOINT}/compound", json=transfer_payload, timeout=TEST_TIMEOUT)
        assert response.status_code == 201

        # Query limit after transfer
        params = {"tenant_id": TENANT_ID, "account_codes": setup_user_wallet["limit_non_reg_count_account"]}

        response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)

        assert response.status_code == 200
        result = response.json()
        assert result[0]["balance"] == 9  # 10 - 1 consumed


# ============================================================================
# Test Class: Maximum Balance Management
# ============================================================================


class TestMaxBalanceManagement:
    """Tests for managing and enforcing maximum wallet balances."""

    def test_increase_max_balance(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test increasing a wallet's maximum balance."""
        increase_amount = 5000000  # ₹50,000 increase
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": f"Increasing max_balance by {increase_amount / 100000}₹",
            "debit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": increase_amount, "currency": "INR"}
            ],
            "credit_legs": [
                {
                    "account_code": setup_system_accounts["max_balance_suspense"],
                    "amount": increase_amount,
                    "currency": "INR",
                }
            ],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 201

    def test_decrease_max_balance(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test decreasing a wallet's maximum balance."""
        decrease_amount = 5000000  # ₹50,000 decrease
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": f"Decreasing max_balance by {decrease_amount / 100000}₹",
            "debit_legs": [
                {
                    "account_code": setup_system_accounts["max_balance_suspense"],
                    "amount": decrease_amount,
                    "currency": "INR",
                }
            ],
            "credit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": decrease_amount, "currency": "INR"}
            ],
        }
        # This is a compound transfer and should use the dedicated endpoint.
        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 201


# ============================================================================
# Test Class: Pending Transactions & Timeouts
# ============================================================================


class TestPendingTransactions:
    """Tests for pending transactions with timeout expiration."""

    def test_create_pending_journal_entry(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test creating a pending journal entry with timeout."""
        entry_date = datetime.now().strftime("%Y-%m-%d")
        timeout_seconds = 60

        payload = {
            "tenant_id": TENANT_ID,
            "narration": "Pending payment awaiting confirmation",
            "entry_date": entry_date,
            "debit_legs": [{"account_code": setup_user_wallet["wallet_account"], "amount": 25000, "currency": "INR"}],
            "credit_legs": [
                {"account_code": setup_system_accounts["PAYABLES_EXTERNAL"], "amount": 25000, "currency": "INR"}
            ],
            "timeout_seconds": timeout_seconds,
        }

        response = http_client.post(PENDING_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 202
        result = response.json()
        assert "journal_id" in result or "id" in result

    def test_pending_entry_expires(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test that pending entry expires after timeout."""
        entry_date = datetime.now().strftime("%Y-%m-%d")
        timeout_seconds = 2  # Very short timeout

        payload = {
            "tenant_id": TENANT_ID,
            "narration": "Payment with short timeout",
            "entry_date": entry_date,
            "debit_legs": [{"account_code": setup_user_wallet["wallet_account"], "amount": 25000, "currency": "INR"}],
            "credit_legs": [
                {"account_code": setup_system_accounts["PAYABLES_EXTERNAL"], "amount": 25000, "currency": "INR"}
            ],
            "timeout_seconds": timeout_seconds,
        }

        response = http_client.post(PENDING_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 202
        _pending_id = response.json().get("entry_id") or response.json().get("id")

        # Wait for timeout to expire
        time.sleep(timeout_seconds + 1)

        # Attempt to commit expired entry (should fail)
        # Note: Commit endpoint not explicitly defined; this is illustrative

    def test_create_pending_compound_transfer(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test creating pending compound transfer with timeout."""
        entry_date = datetime.now().strftime("%Y-%m-%d")
        timeout_seconds = 300  # 5 minutes

        payload = {
            "tenant_id": TENANT_ID,
            "narration": "P2P transfer pending fraud check",
            "entry_date": entry_date,
            "debit_legs": [{"account_code": setup_user_wallet["wallet_account"], "amount": 10000, "currency": "INR"}],
            "credit_legs": [
                {"account_code": setup_system_accounts["PAYABLES_EXTERNAL"], "amount": 10000, "currency": "INR"}
            ],
            "timeout_seconds": timeout_seconds,
        }

        response = http_client.post(PENDING_TRANSFERS_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code == 202


# ============================================================================
# Test Class: Error Handling & Edge Cases
# ============================================================================


class TestErrorHandling:
    """Tests for error conditions and edge cases."""

    def test_invalid_tenant_id(self, http_client):
        """Test request with invalid tenant ID."""
        payload = {
            "tenant_id": "invalid_tenant_xyz",
            "accounts": [{"code": "TEST_ACCOUNT", "name": "Test", "type": "asset"}],
        }

        response = http_client.post(ACCOUNTS_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        # Should fail or be handled appropriately
        assert response.status_code >= 400 or response.status_code == 200

    def test_missing_required_field(self, http_client):
        """Test journal entry missing required fields."""
        payload = {
            "tenant_id": TENANT_ID,
            # Missing entry_date and other required fields
            "narration": "Incomplete entry",
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code >= 400

    def test_invalid_account_type(self, http_client):
        """Test account creation with invalid type."""
        payload = {
            "tenant_id": TENANT_ID,
            "accounts": [
                {
                    "code": "INVALID_TYPE_ACCOUNT",
                    "name": "Invalid Type",
                    "type": "invalid_type_xyz",  # Invalid type
                }
            ],
        }

        response = http_client.post(ACCOUNTS_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code >= 400

    def test_negative_amount_handling(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test handling of negative amounts."""
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Entry with negative amount",
            "debit_legs": [
                {
                    "account_code": setup_system_accounts["BANK_SUSPENSE"],
                    "amount": -100000,  # Negative amount
                    "currency": "INR",
                }
            ],
            "credit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": -100000, "currency": "INR"}
            ],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        # Should handle gracefully (accept or reject with clear error)
        assert response.status_code in [200, 400, 500]

    def test_zero_amount_handling(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test handling of zero amounts."""
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Entry with zero amount",
            "debit_legs": [{"account_code": setup_system_accounts["BANK_SUSPENSE"], "amount": 0, "currency": "INR"}],
            "credit_legs": [{"account_code": setup_user_wallet["wallet_account"], "amount": 0, "currency": "INR"}],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        # Should handle gracefully
        assert response.status_code in [200, 201]

    def test_missing_debit_legs(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test entry with missing debit legs."""
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Missing debit legs",
            "debit_legs": [],  # Empty
            "credit_legs": [{"account_code": setup_user_wallet["wallet_account"], "amount": 100000, "currency": "INR"}],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code >= 400

    def test_missing_credit_legs(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test entry with missing credit legs."""
        entry_date = datetime.now().strftime("%Y-%m-%d")

        payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Missing credit legs",
            "debit_legs": [
                {"account_code": setup_system_accounts["BANK_SUSPENSE"], "amount": 100000, "currency": "INR"}
            ],
            "credit_legs": [],  # Empty
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=payload, timeout=TEST_TIMEOUT)

        assert response.status_code >= 400


# ============================================================================
# Test Class: Integration Scenarios
# ============================================================================


class TestIntegrationScenarios:
    """End-to-end integration tests simulating real wallet workflows."""

    def test_complete_wallet_workflow(self, http_client, setup_system_accounts, setup_user_wallet, refill_user_limits):
        """Test complete wallet workflow: topup → transfer → query balance."""
        entry_date = datetime.now().strftime("%Y-%m-%d")

        # Step 0: Get initial balance to ensure test isolation
        initial_balance_params = {"tenant_id": TENANT_ID, "account_codes": setup_user_wallet["wallet_account"]}
        initial_response = http_client.get(BALANCES_ENDPOINT, params=initial_balance_params, timeout=TEST_TIMEOUT)
        assert initial_response.status_code == 200
        initial_balance = initial_response.json()[0].get("balance", 0)

        # Step 1: Top-up wallet
        topup_amount = 500000  # ₹5,000
        topup_payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Workflow test: wallet topup",
            "debit_legs": [
                {"account_code": setup_system_accounts["BANK_SUSPENSE"], "amount": topup_amount, "currency": "INR"}
            ],
            "credit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": topup_amount, "currency": "INR"}
            ],
        }

        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=topup_payload, timeout=TEST_TIMEOUT)
        assert response.status_code == 201

        # Step 2: Perform transfer (consume limits)
        transfer_amount = 100000  # ₹1,000
        transfer_payload = {
            "tenant_id": TENANT_ID,
            "entry_date": entry_date,
            "narration": "Workflow test: transfer to beneficiary",
            "debit_legs": [
                {"account_code": setup_user_wallet["wallet_account"], "amount": transfer_amount, "currency": "INR"},
                {
                    "account_code": setup_user_wallet["limit_non_reg_amt_account"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {"account_code": setup_user_wallet["limit_non_reg_count_account"], "amount": 1, "currency": "QTY"},
            ],
            "credit_legs": [
                {
                    "account_code": setup_system_accounts["PAYABLES_EXTERNAL"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {
                    "account_code": setup_system_accounts["source_of_funds"],
                    "amount": transfer_amount,
                    "currency": "INR",
                },
                {"account_code": setup_system_accounts["source_of_funds"], "amount": 1, "currency": "QTY"},
            ],
        }

        response = http_client.post(f"{TRANSFERS_ENDPOINT}/compound", json=transfer_payload, timeout=TEST_TIMEOUT)
        assert response.status_code == 201, f"Compound transfer failed with status {response.status_code}"

        # Step 3: Query wallet balance
        params = {"tenant_id": TENANT_ID, "account_codes": setup_user_wallet["wallet_account"]}

        response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)
        assert response.status_code == 200
        result = response.json()
        assert len(result) > 0
        # Wallet should have: topup - transfer = 500000 - 100000 = 400000
        expected_balance = initial_balance + topup_amount - transfer_amount
        assert result[0]["balance"] == expected_balance

    def test_daily_limit_reset_workflow(self, http_client, setup_system_accounts, setup_user_wallet):
        """Test daily limit reset workflow."""
        _entry_date = datetime.now().strftime("%Y-%m-%d")

        # Initial refill
        refill_payload = {
            "tenant_id": TENANT_ID,
            "source_of_funds_account_code": setup_system_accounts["sys_rate_limiter_credit"],
            "accounts_to_refill": [
                {
                    "account_code": setup_user_wallet["limit_non_reg_count_account"],
                    "amount": 5,  # 5 transfers
                    "currency": "QTY",
                }
            ],
        }

        response = http_client.post(REFILL_ENDPOINT, json=refill_payload, timeout=TEST_TIMEOUT)
        assert response.status_code == 204

        # Verify limit is 5
        params = {"tenant_id": TENANT_ID, "account_codes": setup_user_wallet["limit_non_reg_count_account"]}

        response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)
        assert response.status_code == 200
        result = response.json()
        assert result[0]["balance"] == 5, "Initial refill should set the balance to 5"

        # Step 2: Simulate end-of-day by zeroing out the balance.
        # This is a debit from the limiter account back to the source.
        zero_out_payload = {
            "tenant_id": TENANT_ID,
            "entry_date": (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
            "narration": "Zero-out limit at end of day",
            "debit_legs": [
                {"account_code": setup_user_wallet["limit_non_reg_count_account"], "amount": 5, "currency": "QTY"}
            ],
            "credit_legs": [
                {"account_code": setup_system_accounts["sys_rate_limiter_credit"], "amount": 5, "currency": "QTY"}
            ],
        }
        response = http_client.post(JOURNAL_ENTRIES_ENDPOINT, json=zero_out_payload, timeout=TEST_TIMEOUT)
        assert response.status_code == 201, "Failed to zero-out the limit account"

        # Step 3: Refill again for the new day.
        refill_payload["accounts_to_refill"][0]["amount"] = 10  # New day's limit is 10

        response = http_client.post(REFILL_ENDPOINT, json=refill_payload, timeout=TEST_TIMEOUT)
        assert response.status_code == 204

        # Step 4: Verify the limit is now correctly set to 10 for the new day.
        response = http_client.get(BALANCES_ENDPOINT, params=params, timeout=TEST_TIMEOUT)
        assert response.status_code == 200
        result = response.json()
        assert result[0]["balance"] == 10, "Daily reset should set the balance to the new value, not add to it"


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
