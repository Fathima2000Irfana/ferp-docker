#!/usr/bin/env python3
import os
import sys
import tigerbeetle as tb
from tigerbeetle import QueryFilter, QueryFilterFlags

def get_all_accounts():
    with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESSES", "3001,3002")) as client:
        all_accounts = []
        timestamp_min = 0

        while True:
            query_filter = QueryFilter(
                user_data_128=0,
                user_data_64=0,
                user_data_32=0,
                code=0,
                ledger=0,
                timestamp_min=timestamp_min,
                timestamp_max=0,
                limit=8189,
                flags=0,
            )

            accounts_batch = client.query_accounts(query_filter)
            if not accounts_batch:
                break

            all_accounts.extend(accounts_batch)
            timestamp_min = accounts_batch[-1].timestamp + 1

        return all_accounts


def get_account_by_id(account_id_str):
    with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESSES", "3001,3002")) as client:
        # Parse account ID as u128 (TigerBeetle uses 128-bit IDs)
        try:
            account_id = int(account_id_str)
        except ValueError:
            print(f"Error: Account ID must be a valid integer, got '{account_id_str}'", file=sys.stderr)
            sys.exit(1)

        # Query for a single account by ID
        query_filter = QueryFilter(
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            code=0,
            ledger=0,
            timestamp_min=0,
            timestamp_max=0,
            limit=8189,
            flags=0,
        )

        accounts = client.query_accounts(query_filter)
        for acc in accounts:
            if acc.id == account_id:
                return acc

        return None


def print_account(account):
    balance = account.credits_posted - account.debits_posted

    print(f"Account ID: {account.id}")
    print(f"  Ledger: {account.ledger}")
    print(f"  Code: {account.code}")
    print(f"  User Data:")
    print(f"    user_data_128: {account.user_data_128}")
    print(f"    user_data_64:  {account.user_data_64}")
    print(f"    user_data_32:  {account.user_data_32}")

    print(f"  Balances:")
    print(f"    debits_posted:   {account.debits_posted}")
    print(f"    credits_posted:  {account.credits_posted}")
    print(f"    debits_pending:  {account.debits_pending}")
    print(f"    credits_pending: {account.credits_pending}")
    print(f"    Balance (posted): {balance}")

#     print(f"  Limits:")
#     print(f"    debits_max:   {account.debits_max}")
#     print(f"    credits_max:  {account.credits_max}")

    print(f"  Timestamps:")
    print(f"    timestamp: {account.timestamp}")

    print(f"  Flags:")
    print(f"    flags: {account.flags}")

    # Decode common flags (adjust based on your flag constants)
    flags = account.flags
    print(f"  Flags (decoded):")
    print(f"    locked: {bool(flags & 1)}")
    print(f"    closed: {bool(flags & 2)}")
    print(f"    debit_must_not_exceed_credits: {bool(flags & 4)}")
    print(f"    credit_must_not_exceed_debits: {bool(flags & 8)}")
    print(f"    debit_reserved: {bool(flags & 16)}")
    print(f"    credit_reserved: {bool(flags & 32)}")
    print(f"    debit_reserved_pending: {bool(flags & 64)}")
    print(f"    credit_reserved_pending: {bool(flags & 128)}")

    print("-" * 60)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # No argument: list all accounts
        accounts = get_all_accounts()
        print(f"Retrieved {len(accounts)} accounts.\n")
        for account in accounts:
            print_account(account)
    elif len(sys.argv) == 2:
        # One argument: show only that account
        account_id_str = sys.argv[1]
        account = get_account_by_id(account_id_str)
        if account is None:
            print(f"Account with ID {account_id_str} not found.", file=sys.stderr)
            sys.exit(1)
        print(f"Found 1 account:\n")
        print_account(account)
    else:
        print(f"Usage: {sys.argv[0]} [account_id]", file=sys.stderr)
        print(f"  With no argument: list all accounts", file=sys.stderr)
        print(f"  With account_id: show only that account", file=sys.stderr)
        sys.exit(1)
