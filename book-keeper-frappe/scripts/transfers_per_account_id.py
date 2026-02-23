import tigerbeetle as tb
import os
from typing import Optional
from datetime import datetime, timezone


def print_account_transfers(client, account_id: int, limit: int = 100):
    """
    Fetch and print all transfers for a given account ID.
    Args:
        client: TigerBeetle client instance
        account_id: The account ID to query
        limit: Maximum number of transfers to fetch (default: 100)
    """
    # Create the account filter
    filter = tb.AccountFilter(
        account_id=account_id,
        user_data_128=0,  # No filter by UserData
        user_data_64=0,
        user_data_32=0,
        code=0,  # No filter by Code
        timestamp_min=0,  # No filter by Timestamp
        timestamp_max=0,  # No filter by Timestamp
        limit=limit,
        flags=tb.AccountFilterFlags.DEBITS |  # Include transfers from the debit side
              tb.AccountFilterFlags.CREDITS |  # Include transfers from the credit side
              tb.AccountFilterFlags.REVERSED  # Sort by timestamp in reverse-chronological order
    )
    # Fetch the transfers
    account_transfers = client.get_account_transfers(filter)
    # Print header
    print(f"\n{'='*120}")
    print(f"Account Transfers for Account ID: {account_id}")
    print(f"Total Transfers Found: {len(account_transfers)}")
    print(f"{'='*120}")
    if not account_transfers:
        print("No transfers found for this account.")
        return
    # Print table header
    print(f"\n{'Transfer ID':<20} {'Debit Acc':<25} {'Credit Acc':<25} {'Amount':>15} {'Code':<10} {'Timestamp':<20} {'Flags':<30}")
    print(f"{'-'*120}")
    # Print each transfer
    for transfer in account_transfers:
        # Determine transfer direction
        if transfer.debit_account_id == account_id:
            direction = "DEBIT (out)"
            amount_display = f"-{transfer.amount:,}"
        else:
            direction = "CREDIT (in)"
            amount_display = f"+{transfer.amount:,}"
        # Format flags
        flags_str = format_transfer_flags(transfer.flags)

        ts_seconds = int(transfer.timestamp / 1_000_000_000)
        dt_utc = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
        print(f"{str(transfer.id):<20} "
              f"{transfer.debit_account_id:<25} "
              f"{transfer.credit_account_id:<25} "
              f"{amount_display:>15} "
              f"{transfer.code:<10} "
              f"{dt_utc.isoformat()} "
              f"{flags_str:<30}")
    # Print summary
    print(f"{'-'*120}")
    print_transfer_summary(account_transfers, account_id)
    print(f"{'='*120}\n")


def format_transfer_flags(flags):
    """Format transfer flags into readable string."""
    flag_list = []
    if flags & tb.TransferFlags.LINKED:
        flag_list.append("LINKED")
    if flags & tb.TransferFlags.PENDING:
        flag_list.append("PENDING")
    if flags & tb.TransferFlags.POST_PENDING_TRANSFER:
        flag_list.append("POST_PENDING")
    if flags & tb.TransferFlags.VOID_PENDING_TRANSFER:
        flag_list.append("VOID_PENDING")
    if flags & tb.TransferFlags.BALANCING_DEBIT:
        flag_list.append("BALANCING_DEBIT")
    if flags & tb.TransferFlags.BALANCING_CREDIT:
        flag_list.append("BALANCING_CREDIT")
    return ", ".join(flag_list) if flag_list else "NONE"


def print_transfer_summary(transfers, account_id):
    """Print summary statistics for the transfers."""
    total_debits = 0
    total_credits = 0
    for transfer in transfers:
        if transfer.debit_account_id == account_id:
            total_debits += transfer.amount
        else:
            total_credits += transfer.amount
    net_balance = total_credits - total_debits
    print(f"\nSummary:")
    print(f"  Total Credits (In):  +{total_credits:,}")
    print(f"  Total Debits (Out):  -{total_debits:,}")
    print(f"  Net Change:          {net_balance:+,}")


# Synchronous version
def main_sync(account_id: int, limit: int = 100):
    """
    Main function using synchronous TigerBeetle client.
    Args:
        account_id: The account ID to query
        limit: Maximum number of transfers to fetch
    """
    with tb.ClientSync(
        cluster_id=42,
        replica_addresses=os.getenv("TB_ADRESSES", "3001,3002")
    ) as client:
        print_account_transfers(client, account_id, limit)


# Asynchronous version
async def main_async(account_id: int, limit: int = 100):
    """
    Main function using asynchronous TigerBeetle client.
    Args:
        account_id: The account ID to query
        limit: Maximum number of transfers to fetch
    """
    async with tb.ClientAsync(
        cluster_id=42,
        replica_addresses=os.getenv("TB_ADRESSES", "3001,3002")
    ) as client:
        print_account_transfers(client, account_id, limit)


# Example usage
if __name__ == "__main__":
    import sys

    # Get account ID from command line or use default
    if len(sys.argv) > 1:
        account_id = int(sys.argv[1])
    else:
        account_id = 2  # Default account ID

    # Get limit from command line or use default
    if len(sys.argv) > 2:
        limit = int(sys.argv[2])
    else:
        limit = 100

    # Run synchronous version
    print("Running synchronous client...")
    main_sync(account_id, limit)
    # Uncomment below to run async version
    # import asyncio
    # print("\nRunning asynchronous client...")
    # asyncio.run(main_async(account_id, limit))
