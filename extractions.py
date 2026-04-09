"""
Extractions Loader
Provides easy access to the most recent Best Data extraction results.

Usage:
    import sys
    sys.path.append(r"C:\path\to\extraction-tool")  # your local sync path
    from extractions import get_extraction, list_plans

    # Long format (one row per participant per element)
    df = get_extraction("Plan_Name")

    # Wide format (one row per participant, all elements as columns)
    df_wide = get_extraction("Plan_Name", wide=True)

    # See available plans
    list_plans()
"""

import pickle
import pandas as pd
from pathlib import Path


# Default output folder relative to this module's location
_DEFAULT_OUTPUT_FOLDER = Path(__file__).parent / "output"

# Metadata columns to pivot alongside each element's value in wide format
_META_COLS = ["Value", "Cleaned Value", "DocID", "Page Number", "Best Source", "Document Link", "Notes"]


def get_extraction(plan_name: str, wide: bool = False, output_folder: str = None) -> pd.DataFrame:
    """
    Load the most recent Best Data extraction results for a plan.

    Args:
        plan_name:      Name of the plan (must match the plan folder name exactly).
        wide:           If True, pivot to wide format — one row per Participant ID,
                        with each element's columns as {Element} {Column}.
                        If False (default), return long format as-is.
        output_folder:  Path to the output folder. Defaults to the 'output' folder
                        inside the extraction-tool directory.

    Returns:
        DataFrame with Best Data extraction results.

    Raises:
        FileNotFoundError: If no best data pickle exists for the given plan name.
    """
    folder = Path(output_folder) if output_folder else _DEFAULT_OUTPUT_FOLDER

    if not folder.exists():
        raise FileNotFoundError(
            f"Output folder not found: {folder}\n"
            f"Check that your local sync path is correct or pass output_folder explicitly."
        )

    pattern = f"{plan_name}_INTERACTIVE_REPORT_*_best_data.pkl"
    matches = sorted(folder.glob(pattern))

    if not matches:
        available = _available_plans(folder)
        hint = f"\nAvailable plans: {', '.join(available)}" if available else "\nNo plans found in output folder."
        raise FileNotFoundError(
            f"No best data found for plan '{plan_name}' in {folder}.{hint}"
        )

    # Most recent file by filename timestamp (they sort lexicographically)
    latest = matches[-1]

    with open(latest, "rb") as f:
        df = pickle.load(f)

    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    if wide:
        df = _to_wide(df)

    return df


def list_plans(output_folder: str = None) -> None:
    """
    Print all plans that have best data pickle files in the output folder,
    along with the timestamp of each plan's most recent extraction.

    Args:
        output_folder: Path to the output folder. Defaults to the 'output' folder
                       inside the extraction-tool directory.
    """
    folder = Path(output_folder) if output_folder else _DEFAULT_OUTPUT_FOLDER

    if not folder.exists():
        print(f"Output folder not found: {folder}")
        return

    matches = sorted(folder.glob("*_INTERACTIVE_REPORT_*_best_data.pkl"))

    if not matches:
        print("No extraction results found in output folder.")
        return

    # Group by plan name and show latest timestamp per plan
    plans: dict[str, str] = {}
    for f in matches:
        # Filename: {plan_name}_INTERACTIVE_REPORT_{timestamp}_best_data.pkl
        parts = f.stem.split("_INTERACTIVE_REPORT_")
        if len(parts) != 2:
            continue
        plan = parts[0]
        timestamp = parts[1].replace("_best_data", "")
        plans[plan] = timestamp  # sorted ascending so last wins = most recent

    print(f"{'Plan':<40} {'Latest Extraction'}")
    print("-" * 60)
    for plan, ts in sorted(plans.items()):
        # Format timestamp: 20240406_143022 → 2024-04-06 14:30:22
        try:
            formatted = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]} {ts[9:11]}:{ts[11:13]}:{ts[13:15]}"
        except Exception:
            formatted = ts
        print(f"{plan:<40} {formatted}")


def _available_plans(folder: Path) -> list[str]:
    """Return list of plan names that have pkl files in the output folder."""
    plans = set()
    for f in folder.glob("*_INTERACTIVE_REPORT_*_best_data.pkl"):
        parts = f.stem.split("_INTERACTIVE_REPORT_")
        if len(parts) == 2:
            plans.add(parts[0])
    return sorted(plans)


def _to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot long Best Data to wide format.

    Each element becomes a group of columns: {Element} Value, {Element} Cleaned Value,
    {Element} DocID, {Element} Page Number, {Element} Best Source,
    {Element} Document Link, {Element} Notes.

    One row per Participant ID.
    """
    if df.empty:
        return df

    present_meta = [c for c in _META_COLS if c in df.columns]
    id_col = "Participant ID"

    if id_col not in df.columns or "Element" not in df.columns:
        return df

    pivot_parts = []
    for element, group in df.groupby("Element", sort=False):
        sub = group[[id_col] + present_meta].copy()
        sub = sub.rename(columns={col: f"{element} {col}" for col in present_meta})
        sub = sub.set_index(id_col)
        pivot_parts.append(sub)

    if not pivot_parts:
        return df

    wide = pd.concat(pivot_parts, axis=1).reset_index()
    wide = wide.rename(columns={id_col: id_col})  # keep name clean

    # Sort columns: group by element, preserving element order from data
    elements = df["Element"].unique()
    ordered_cols = [id_col]
    for element in elements:
        for meta in present_meta:
            col = f"{element} {meta}"
            if col in wide.columns:
                ordered_cols.append(col)

    wide = wide[[c for c in ordered_cols if c in wide.columns]]

    return wide
