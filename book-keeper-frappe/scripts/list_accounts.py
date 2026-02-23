import os
import tigerbeetle as tb
from tigerbeetle import QueryFilter, QueryFilterFlags

def get_all_accounts():
    # Use ClientSync for synchronous operations
    with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESSES", "3001,3002")) as client:
        all_accounts = []
        # Start with a minimum timestamp of 0
        timestamp_min = 0

        while True:
            # Configure the query filter for pagination
            # We use no specific filters other than timestamp and limit
            query_filter = QueryFilter(
                user_data_128=0,
                user_data_64=0,
                user_data_32=0,
                code=0,
                ledger=0, # No filter by ledger
                timestamp_min=timestamp_min,
                timestamp_max=0, # No max timestamp filter
                limit=8189, # Max limit per batch
                flags=0,
            )

            # Query accounts
            accounts_batch = client.query_accounts(query_filter)
            if not accounts_batch:
                # If the batch is empty, we have retrieved all accounts
                break

            all_accounts.extend(accounts_batch)

            # For the next iteration, set the minimum timestamp to the timestamp of the last account
            # in the current batch to continue paging
            timestamp_min = accounts_batch[-1].timestamp + 1

            # Note: The result is always limited in size.
            # If there are more results, you need to page through them
            # using the QueryFilter's timestamp_min.
            # The client automatically handles the internal details of this pagination flow.

        return all_accounts

if __name__ == "__main__":
    accounts = get_all_accounts()
    print(f"Retrieved {len(accounts)} accounts.")
    for account in accounts:
        balance = account.credits_posted - account.debits_posted
        if balance == -20000000 or balance == 0 or balance == 10 or balance > -19998800:
              print("hmmm")
        print(f"Account ID: {account.id}, Ledger: {account.ledger}, Code: {account.code}, Balance Posted: {balance}")


