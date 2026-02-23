import json
import os
import sys
import uuid

import httpx

# --- Configuration ---
HOST = os.getenv("BOOKKEEPER_HOST", "http://localhost:9090")
API_PREFIX = "/api/book-keeper/v1"
TENANT_ID = "twophasetenant"
HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

# --- Colors for logging ---
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
RED = "\033[0;31m"
NC = "\033[0m"


# --- Helper Functions ---
def log_info(message):
    print(f"{YELLOW}[INFO] {message}{NC}")


def log_success(message):
    print(f"{GREEN}[SUCCESS] {message}{NC}")


def log_error(message, response=None):
    print(f"{RED}[ERROR] {message}{NC}", file=sys.stderr)
    if response:
        try:
            # Try to pretty-print JSON if possible
            error_details = json.dumps(response.json(), indent=2)
            print(f"{RED}Response Body:\n{error_details}{NC}", file=sys.stderr)
        except (json.JSONDecodeError, AttributeError):
            print(f"{RED}Response Body:\n{response.text}{NC}", file=sys.stderr)
    sys.exit(1)


def make_request(method, endpoint, expected_status, payload=None, params=None):
    url = f"{HOST}{API_PREFIX}{endpoint}"
    log_info(f"{method.upper()} to {endpoint} (Expecting {expected_status})...")

    try:
        with httpx.Client(headers=HEADERS, timeout=10.0) as client:
            if method.upper() == "POST":
                response = client.post(url, json=payload)
            elif method.upper() == "GET":
                response = client.get(url, params=params)
            else:
                log_error(f"Unsupported HTTP method: {method}")

            if response.status_code != expected_status:
                log_error(
                    f"Expected status {expected_status} but got {response.status_code} for {method.upper()} {endpoint}",
                    response,
                )

            log_success(f"Received expected status {response.status_code}")
            return response
    except httpx.ConnectError as e:
        log_error(f"Connection to {HOST} failed. Is the service running? Details: {e}")
    except Exception as e:
        log_error(f"An unexpected error occurred: {e}")


# --- Main Test Logic ---
def main():
    log_info("Starting Book-Keeper API test script...")
    run_id = uuid.uuid4().hex[:8]

    tested_features = [
        "Accounts",
        "Journal Entries",
        "Pending Entries",
        "Balances",
        "Refill",
        "Correction",
        "Errors",
        "Pending Compound Entries",
        "Pending Transfer Timeout",
    ]

    # Define a mapping for unique account codes for this test run
    account_codes = {
        "cash": f"1001_{run_id}",
        "revenue": f"4001_{run_id}",
        "wallet1": f"Wallet-Cust001_{run_id}",
        "wallet2": f"Wallet-Cust002_{run_id}",
        "bank_suspense": f"Bank-Suspense_{run_id}",
        "payables_ext": f"Payables-External_{run_id}",
        "sof": f"Source-of-Funds_{run_id}",
        "limiter_debit": f"sys_rate_limiter_debit_{run_id}",
        "limiter_credit": f"sys_rate_limiter_credit_{run_id}",
        "ctrl_compound": f"sys_ctrl_compound_{run_id}",
    }

    # 1. Idempotently create required accounts
    accounts_payload = {
        "tenant_id": TENANT_ID,
        "accounts": [
            {"code": account_codes["cash"], "name": "Cash", "type": "asset"},
            {"code": account_codes["revenue"], "name": "Sales Revenue", "type": "revenue"},
            {"code": account_codes["bank_suspense"], "name": "Bank Suspense", "type": "asset"},
            {"code": account_codes["payables_ext"], "name": "External Payables", "type": "liability"},
            {"code": account_codes["sof"], "name": "Source of Funds", "type": "liability"},
            {
                "code": account_codes["wallet1"],
                "name": "Customer 001 Wallet",
                "type": "liability",
                "max_balance": 200000,
                "currency": "USD",
            },
            {
                "code": account_codes["wallet2"],
                "name": "Customer 002 Wallet",
                "type": "liability",
                "max_balance": 200000,
                "currency": "USD",
            },
            # System accounts for rate limiting
            {"code": account_codes["limiter_debit"], "name": "Rate Limiter Debit", "type": "asset"},
            {"code": account_codes["limiter_credit"], "name": "Rate Limiter Credit", "type": "liability"},
            # System account for compound transfers
            {"code": account_codes["ctrl_compound"], "name": "Compound Transfer Control", "type": "liability"},
        ],
    }
    make_request("POST", "/accounts", 200, payload=accounts_payload)

    # 2. Test: Single-Phase (Standard) Journal Entry
    log_info("--- Running SINGLE-PHASE COMMIT Scenario ---")
    single_phase_id = str(uuid.uuid4())
    single_phase_payload = {
        "tenant_id": TENANT_ID,
        "idempotency_key": single_phase_id,
        "entry_date": "2024-01-01",
        "narration": "Standard single-phase sale",
        "debit_legs": [{"account_code": account_codes["cash"], "amount": 1000, "currency": "USD"}],
        "credit_legs": [{"account_code": account_codes["revenue"], "amount": 1000, "currency": "USD"}],
    }
    je_response = make_request("POST", "/journal-entries", 201, payload=single_phase_payload)
    journal_id_for_correction = je_response.json()["journal_id"]

    # 3. Test: Create and Commit a Pending Entry
    log_info("--- Running TWO-PHASE COMMIT Scenario ---")
    idempotency_key_commit = str(uuid.uuid4())
    create_commit_payload = {
        "tenant_id": TENANT_ID,
        "idempotency_key": idempotency_key_commit,
        "entry_date": "2024-01-02",
        "narration": "Test sale to be committed",
        "debit_legs": [{"account_code": account_codes["cash"], "amount": 500, "currency": "USD"}],
        "credit_legs": [{"account_code": account_codes["revenue"], "amount": 500, "currency": "USD"}],
    }
    create_response = make_request("POST", "/pending-journal-entries", 202, payload=create_commit_payload)
    commit_journal_id = create_response.json()["journal_id"]

    commit_payload = {"tenant_id": TENANT_ID}
    make_request("POST", f"/pending-journal-entries/{commit_journal_id}/commit", 200, payload=commit_payload)

    # 4. Test: Create and Void a Pending Entry
    log_info("--- Running TWO-PHASE VOID Scenario ---")
    idempotency_key_void = str(uuid.uuid4())
    create_void_payload = {
        "tenant_id": TENANT_ID,
        "idempotency_key": idempotency_key_void,
        "entry_date": "2024-01-03",
        "narration": "Test sale to be voided",
        "debit_legs": [{"account_code": account_codes["cash"], "amount": 250, "currency": "USD"}],
        "credit_legs": [{"account_code": account_codes["revenue"], "amount": 250, "currency": "USD"}],
    }
    create_response_void = make_request("POST", "/pending-journal-entries", 202, payload=create_void_payload)
    void_journal_id = create_response_void.json()["journal_id"]

    void_payload = {"tenant_id": TENANT_ID}
    make_request("POST", f"/pending-journal-entries/{void_journal_id}/void", 200, payload=void_payload)

    # 4a. Test: Create and Commit a Pending COMPOUND Entry
    log_info("--- Running TWO-PHASE COMPOUND COMMIT Scenario ---")
    create_compound_commit_payload = {
        "tenant_id": TENANT_ID,
        "narration": "Test compound sale to be committed",
        "debit_legs": [{"account_code": account_codes["wallet1"], "amount": 150, "currency": "USD"}],
        "credit_legs": [
            {"account_code": account_codes["cash"], "amount": 100, "currency": "USD"},
            {"account_code": account_codes["revenue"], "amount": 50, "currency": "USD"},
        ],
    }
    create_compound_commit_response = make_request(
        "POST", "/pending-compound-transfers", 202, payload=create_compound_commit_payload
    )
    compound_commit_journal_id = create_compound_commit_response.json()["journal_id"]
    make_request(
        "POST",
        f"/pending-compound-transfers/{compound_commit_journal_id}/commit",
        200,
        payload={"tenant_id": TENANT_ID},
    )

    # 4b. Test: Create and Void a Pending COMPOUND Entry
    log_info("--- Running TWO-PHASE COMPOUND VOID Scenario ---")
    create_compound_void_payload = create_compound_commit_payload.copy()
    create_compound_void_response = make_request(
        "POST",
        "/pending-compound-transfers",
        202,
        payload=create_compound_void_payload,
    )
    compound_void_journal_id = create_compound_void_response.json()["journal_id"]
    make_request(
        "POST", f"/pending-compound-transfers/{compound_void_journal_id}/void", 200, payload={"tenant_id": TENANT_ID}
    )

    # 5. Test: Balance Queries
    log_info("--- Running BALANCE QUERY Scenario ---")
    balance_params = (
        f"?tenant_id={TENANT_ID}&account_codes={account_codes['cash']}&"
        f"account_codes={account_codes['revenue']}&account_codes={account_codes['wallet1']}"
    )
    balance_response = make_request("GET", f"/accounts/balances{balance_params}", 200)
    balances = balance_response.json()
    log_info(f"Retrieved {len(balances)} account balances")

    # 6. Test: Refill Rate Limiter (Admin)
    log_info("--- Running REFILL RATE LIMITER Scenario ---")
    refill_payload = {
        "tenant_id": TENANT_ID,
        "source_of_funds_account_code": account_codes["limiter_credit"],
        "accounts_to_refill": [{"account_code": account_codes["limiter_debit"], "amount": 10000, "currency": "USD"}],
    }
    make_request("POST", "/admin/limiter-accounts/refill", 204, payload=refill_payload)

    # 7. Test: Correction Entry (Admin)
    log_info("--- Running CORRECTION ENTRY Scenario ---")
    correction_response = make_request(
        "POST", f"/admin/journal-entries/{journal_id_for_correction}/correct?tenant_id={TENANT_ID}", 200
    )
    reversal_id = correction_response.json()["reversal_journal_id"]
    log_info(f"Created reversal journal entry: {reversal_id}")

    # 7b. Test: Compound Journal Entry Reversal (Admin)
    log_info("--- Running COMPOUND CORRECTION ENTRY Scenario ---")

    # First, create a standard compound journal entry (Multiple credits)
    compound_je_id = str(uuid.uuid4())
    compound_payload = {
        "tenant_id": TENANT_ID,
        "idempotency_key": compound_je_id,
        "entry_date": "2024-01-06",
        "narration": "Compound sale for correction",
        "debit_legs": [{"account_code": account_codes["cash"], "amount": 200, "currency": "USD"}],
        "credit_legs": [
            {"account_code": account_codes["revenue"], "amount": 150, "currency": "USD"},
            {"account_code": account_codes["wallet1"], "amount": 50, "currency": "USD"},
        ],
    }

    # POST the compound entry
    comp_resp = make_request("POST", "/journal-entries", 201, payload=compound_payload)
    journal_id_to_reverse = comp_resp.json()["journal_id"]

    # Trigger the correction (reversal)
    correction_params = f"?tenant_id={TENANT_ID}"
    comp_correction_resp = make_request(
        "POST", f"/admin/journal-entries/{journal_id_to_reverse}/correct{correction_params}", 200
    )

    reversal_id = comp_correction_resp.json()["reversal_journal_id"]
    log_success(f"Successfully reversed compound entry {journal_id_to_reverse} with reversal ID: {reversal_id}")

    # 8. Test: Reporting (Optional - depends on projector and TEST_REPORTS env var)
    if os.getenv("TEST_REPORTS", "false").lower() in ("true", "1", "yes"):
        tested_features.append("Reports")
        log_info("--- Running GENERAL LEDGER REPORT Scenario ---")
        gl_params = f"?tenant_id={TENANT_ID}&start_date=2024-01-01&end_date=2024-12-31"
        gl_response = make_request("GET", f"/reports/general-ledger{gl_params}", 200)
        gl_entries = gl_response.json()
        log_info(f"Retrieved {len(gl_entries)} general ledger entries")

        log_info("--- Running TRIAL BALANCE REPORT Scenario ---")
        tb_params = f"?tenant_id={TENANT_ID}&as_of_date=2024-12-31"
        tb_response = make_request("GET", f"/reports/trial-balance{tb_params}", 200)
        tb_entries = tb_response.json()
        log_info(f"Retrieved {len(tb_entries)} trial balance entries")
    else:
        log_info("--- Skipping REPORTING Scenarios (TEST_REPORTS not set) ---")

    # 10. Test: Error Handling - Unbalanced Entry
    log_info("--- Running ERROR HANDLING: Unbalanced Entry ---")
    unbalanced_payload = {
        "tenant_id": TENANT_ID,
        "idempotency_key": str(uuid.uuid4()),
        "entry_date": "2024-01-04",
        "narration": "Unbalanced entry test",
        "debit_legs": [{"account_code": account_codes["cash"], "amount": 100, "currency": "USD"}],
        "credit_legs": [{"account_code": account_codes["revenue"], "amount": 50, "currency": "USD"}],
    }
    make_request("POST", "/journal-entries", 422, payload=unbalanced_payload)
    log_info("Correctly rejected unbalanced entry")

    # 11. Test: Pending Transfer Timeout
    log_info("--- Running PENDING TRANSFER TIMEOUT Scenario ---")
    # Create a pending entry with a short timeout (2 seconds)
    timeout_payload = {
        "tenant_id": TENANT_ID,
        "idempotency_key": str(uuid.uuid4()),
        "entry_date": "2024-01-05",
        "narration": "Timeout test entry",
        "debit_legs": [{"account_code": account_codes["cash"], "amount": 100, "currency": "USD"}],
        "credit_legs": [{"account_code": account_codes["revenue"], "amount": 100, "currency": "USD"}],
        "timeout_seconds": 2,
    }
    timeout_response = make_request("POST", "/pending-journal-entries", 202, payload=timeout_payload)
    timeout_journal_id = timeout_response.json()["journal_id"]
    log_info(f"Created pending entry with 2s timeout: {timeout_journal_id}")

    # Wait for timeout to expire
    log_info("Waiting 5 seconds for timeout to expire...")
    import time

    time.sleep(5)

    # Attempt to commit - should fail
    log_info(f"Attempting to commit expired entry: {timeout_journal_id} (Expecting 500)...")

    make_request("POST", f"/pending-journal-entries/{timeout_journal_id}/commit", 500, payload={"tenant_id": TENANT_ID})
    log_info("Correctly rejected expired pending entry with 500 status.")

    log_info("--- All Scenarios Completed ---")

    print("\n" + "=" * 50)
    log_success("All tests passed! ✅")
    log_info(f"Tested: {', '.join(sorted(tested_features))}")
    print("=" * 50)


if __name__ == "__main__":
    main()
