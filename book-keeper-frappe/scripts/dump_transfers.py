#!/usr/bin/env python3
"""
TigerBeetle Transfer Dumper (Fixed - cluster_id u128)
Dumps all transfers from a TigerBeetle cluster, sorted by timestamp, with optional start time filter.
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from typing import List
import tigerbeetle.client

def parse_timestamp(ts_str: str) -> int:
    """Parse ISO 8601 timestamp to TigerBeetle nanoseconds since epoch."""
    if ts_str == "0":
        return 0
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000_000)

def format_timestamp(ns: int) -> str:
    """Format TigerBeetle nanoseconds to ISO 8601."""
    if ns == 0:
        return "1970-01-01T00:00:00Z"
    dt = datetime.fromtimestamp(ns / 1_000_000_000)
    return dt.isoformat().replace("+00:00", "Z")

def dump_transfers_csv(transfers: List, file_obj):
    """Dump transfers as CSV."""
    if not transfers:
        return
    
    fieldnames = [
        'id', 'debit_account_id', 'credit_account_id', 'user_data_128', 
        'user_data_64', 'user_data_32', 'code', 'ledger', 'amount',
        'pending_id', 'timeout', 'timestamp', 'flags'
    ]
    
    writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
    writer.writeheader()
    
    for t in transfers:
        row = {
            'id': str(t.id),
            'debit_account_id': str(t.debit_account_id),
            'credit_account_id': str(t.credit_account_id),
            'user_data_128': t.user_data_128,
            'user_data_64': t.user_data_64,
            'user_data_32': t.user_data_32,
            'code': t.code,
            'ledger': t.ledger,
            'amount': t.amount,
            'pending_id': str(t.pending_id),
            'timeout': t.timeout,
            'timestamp': format_timestamp(t.timestamp),
            'flags': t.flags,
        }
        writer.writerow(row)

def dump_transfers_jsonl(transfers: List, file_obj):
    """Dump transfers as JSONL (one JSON object per line)."""
    for t in transfers:
        obj = {
            'id': str(t.id),
            'debit_account_id': str(t.debit_account_id),
            'credit_account_id': str(t.credit_account_id),
            'user_data_128': t.user_data_128,
            'user_data_64': t.user_data_64,
            'user_data_32': t.user_data_32,
            'code': t.code,
            'ledger': t.ledger,
            'amount': t.amount,
            'pending_id': str(t.pending_id),
            'timeout': t.timeout,
            'timestamp': format_timestamp(t.timestamp),
            'flags': t.flags,
        }
        file_obj.write(json.dumps(obj) + '\n')
        file_obj.flush()

def main():
    parser = argparse.ArgumentParser(description="Dump all TigerBeetle transfers sorted by timestamp")
    parser.add_argument('--addresses', required=True, help="Comma-separated replica addresses (e.g. 3001 or 3001,3002)")
    parser.add_argument('--cluster-id', type=int, default=0, help="Cluster ID (default: 0)")
    parser.add_argument('--start-time', default="0", help="Start timestamp (ISO 8601 or '0' for beginning, default: 0)")
    parser.add_argument('--output', '-o', default='-', help="Output file (- for stdout, .csv, .jsonl)")
    parser.add_argument('--limit', type=int, default=1000, help="Batch size for queries (default: 1000)")
    parser.add_argument('--dry-run', action='store_true', help="Show query plan without fetching data")
    
    args = parser.parse_args()
    
    # Parse addresses
    addresses = [int(addr.strip()) for addr in args.addresses.split(',')]
    
    # Parse start time
    timestamp_min = parse_timestamp(args.start_time)
    
    print(f"Connecting to TigerBeetle cluster {args.cluster_id} at addresses {addresses}...")
    print(f"Start time: {format_timestamp(timestamp_min)}")
    print(f"Batch size: {args.limit}")
    print()
    
    if args.dry_run:
        print("DRY RUN: Would query transfers with:")
        print(f"  timestamp_min: {timestamp_min}")
        print(f"  timestamp_max: 9223372036854775807 (max)")
        print(f"  limit: {args.limit}")
        return
    
    # FIXED: Convert cluster_id to u128
    cluster_id_u128 = tigerbeetle.client.u128(args.cluster_id)
    
    # Connect to cluster
    client = tigerbeetle.client.Client(addresses, cluster_id_u128)
    
    total_count = 0
    batch_count = 0
    out_file = None
    
    try:
        # FIXED: Use correct QueryTransfersFilter constructor
        filter_ = tigerbeetle.client.QueryTransfersFilter(
            user_data_128=tigerbeetle.client.u128(0),
            user_data_64=0,
            user_data_32=0,
            code=0,
            ledger=0,
            timestamp_min=timestamp_min,
            timestamp_max=(1 << 63) - 1,  # max u64
            limit=args.limit,
            flags=0,  # chronological order
        )
        
        # Determine output format and open file
        if args.output == '-' or args.output.endswith('.jsonl'):
            if args.output == '-':
                out_file = sys.stdout
            else:
                out_file = open(args.output, 'w')
            jsonl_writer = dump_transfers_jsonl
        elif args.output.endswith('.csv'):
            if args.output == '-':
                out_file = sys.stdout
            else:
                out_file = open(args.output, 'w', newline='')
            csv_writer = dump_transfers_csv
        else:
            raise ValueError(f"Unsupported output format: {args.output}")
        
        print(f"Fetching transfers... (output: {args.output})")
        print("-" * 80)
        
        while True:
            transfers = client.query_transfers(filter_)
            
            if not transfers:
                break
            
            # Write batch
            if args.output.endswith('.jsonl') or args.output == '-':
                dump_transfers_jsonl(transfers, out_file)
            else:
                dump_transfers_csv(transfers, out_file)
            
            batch_count += 1
            total_count += len(transfers)
            
            # Progress
            last_ts = transfers[-1].timestamp
            print(f"Batch {batch_count}: {len(transfers)} transfers, "
                  f"last timestamp: {format_timestamp(last_ts)}, "
                  f"total: {total_count}")
            
            # Check if we got a full batch (more to fetch)
            if len(transfers) < args.limit:
                break
            
            # Page to next batch
            filter_.timestamp_min = last_ts + 1
        
        print("-" * 80)
        print(f"Dump complete: {total_count} transfers written to {args.output}")
        
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        try:
            client.close()
        except:
            pass
        if out_file and args.output != '-' and hasattr(out_file, 'close'):
            out_file.close()

if __name__ == "__main__":
    main()

