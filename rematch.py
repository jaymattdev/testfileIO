"""
Rematch Script

Re-runs population matching against one or more existing ADHOC_VALIDATED_*.pkl
files without repeating extraction. Useful when matching logic has changed
(e.g. name normalization improvements) and you want updated match reports from
previously extracted data.

Usage
-----
  # Single pkl
  python rematch.py --config path/to/adhoc_config.toml --output-dir ./output plan_ADHOC_VALIDATED_batch1.pkl

  # Multiple pkls (all batches)
  python rematch.py --config path/to/adhoc_config.toml --output-dir ./output *.pkl

  # Glob all ADHOC_VALIDATED pkls in a folder
  python rematch.py --config path/to/adhoc_config.toml --output-dir ./output --pkl-dir ./output/myfolder

Arguments
---------
  pkl files        One or more ADHOC_VALIDATED_*.pkl paths (positional)
  --pkl-dir        Directory to glob all ADHOC_VALIDATED_*.pkl from (alternative to listing files)
  --config         Path to adhoc_config.toml (required)
  --output-dir     Directory to write the new MATCH_REPORT into (default: same as config dir)
  --name           Plan name prefix for the output file (default: derived from config filename)
"""

import sys
import logging
import argparse
import pickle
from pathlib import Path

import pandas as pd
import toml

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def load_pkl(path: Path) -> pd.DataFrame:
    with open(path, 'rb') as f:
        data = pickle.load(f)
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, list):
        return pd.DataFrame(data)
    raise ValueError(f"Unexpected data type in {path.name}: {type(data)}")


def main():
    parser = argparse.ArgumentParser(
        description='Re-run population matching from existing ADHOC_VALIDATED pkl files.'
    )
    parser.add_argument(
        'pkls', nargs='*',
        help='One or more ADHOC_VALIDATED_*.pkl file paths'
    )
    parser.add_argument(
        '--pkl-dir',
        help='Directory to glob all ADHOC_VALIDATED_*.pkl from (alternative to listing files)'
    )
    parser.add_argument(
        '--config', required=True,
        help='Path to adhoc_config.toml'
    )
    parser.add_argument(
        '--output-dir',
        help='Directory to write MATCH_REPORT into (default: same directory as config)'
    )
    parser.add_argument(
        '--name',
        help='Plan name prefix for the output file (default: config filename stem)'
    )
    args = parser.parse_args()

    # ---- Resolve config -----------------------------------------------------
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: config not found: {config_path}")
        sys.exit(1)

    config_data = toml.load(config_path)

    if not config_data.get('Matching', {}).get('enabled', False):
        print("Warning: Matching is not enabled in the config. Proceeding anyway.")

    plan_name = args.name or config_path.stem
    output_dir = Path(args.output_dir) if args.output_dir else config_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # ---- Resolve pkl files --------------------------------------------------
    pkl_paths = []

    if args.pkl_dir:
        pkl_dir = Path(args.pkl_dir)
        if not pkl_dir.is_dir():
            print(f"Error: --pkl-dir is not a directory: {pkl_dir}")
            sys.exit(1)
        pkl_paths = sorted(pkl_dir.glob('*ADHOC_VALIDATED*.pkl'))
        if not pkl_paths:
            print(f"Error: No ADHOC_VALIDATED_*.pkl files found in {pkl_dir}")
            sys.exit(1)

    if args.pkls:
        for p in args.pkls:
            resolved = Path(p)
            if not resolved.exists():
                print(f"Warning: file not found, skipping: {p}")
                continue
            pkl_paths.append(resolved)

    if not pkl_paths:
        print("Error: No pkl files provided. Use positional arguments or --pkl-dir.")
        sys.exit(1)

    # ---- Load and concatenate -----------------------------------------------
    frames = []
    for pkl_path in pkl_paths:
        print(f"Loading: {pkl_path.name}")
        try:
            df = load_pkl(pkl_path)
            frames.append(df)
            print(f"  {len(df)} rows")
        except Exception as e:
            print(f"  Error loading {pkl_path.name}: {e} — skipping")

    if not frames:
        print("Error: No data loaded from any pkl file.")
        sys.exit(1)

    combined_df = pd.concat(frames, ignore_index=True)
    print(f"\nTotal rows across all pkls: {len(combined_df)}")

    # ---- Run matching -------------------------------------------------------
    from population_matcher import PopulationMatcher

    print("\nLoading reference files...")
    matcher = PopulationMatcher(config=config_data, output_folder=str(output_dir))
    matcher.load_reference_files()

    print("\nRunning population matching...")
    match_report_df = matcher.run(combined_df)

    if match_report_df.empty:
        print("Warning: match report is empty — nothing to save.")
        sys.exit(1)

    # ---- Save ---------------------------------------------------------------
    print("\nSaving match report...")
    match_report_path = matcher.save_match_report(match_report_df, plan_name)

    if match_report_path:
        print(f"\nDone. Match report: {match_report_path}")
    else:
        print("\nError: match report could not be saved.")
        sys.exit(1)


if __name__ == '__main__':
    main()
