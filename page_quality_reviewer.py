"""
Page Quality Reviewer

Reads a PAGE_QUALITY report, finds all flagged pages, and copies their source
files into a review folder for manual inspection.

Output structure:
    plans/{plan_name}/review/
    ├── {SSN}_{DOCID}_Page_{N}.pdf     ← copied from pdf_source_path
    └── text/
        └── {original_text_filename}   ← copied from documents/{batch}/

Usage (standalone):
    python page_quality_reviewer.py --plan my_plan [--report path/to/report.xlsx]
    python page_quality_reviewer.py --plan my_plan --list   # list available reports

Usage (from code):
    from page_quality_reviewer import run_review
    run_review(plan_name="my_plan")
"""

import logging
import shutil
import glob as _glob
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_review(plan_name: str, report_path: str = None, plan_folder: str = None) -> dict:
    """
    Copy flagged pages from the most recent (or specified) PAGE_QUALITY report
    into plans/{plan_name}/review/.

    Args:
        plan_name:    Name of the plan folder under plans/.
        report_path:  Explicit path to a PAGE_QUALITY Excel file. If omitted,
                      uses the most recent one in the plan's output folder.
        plan_folder:  Override the root plan folder path. Defaults to plans/{plan_name}.

    Returns:
        Dict with keys: flagged_count, pdfs_copied, texts_copied, missing_pdfs, missing_texts
    """
    plan_dir    = Path(plan_folder) if plan_folder else Path("plans") / plan_name
    output_dir  = plan_dir / "output"
    review_dir  = plan_dir / "review"
    text_dir    = review_dir / "text"

    review_dir.mkdir(parents=True, exist_ok=True)
    text_dir.mkdir(parents=True, exist_ok=True)

    # ---- Locate report ---------------------------------------------------
    if report_path:
        report_file = Path(report_path)
    else:
        candidates = sorted(output_dir.glob(f"{plan_name}_*_PAGE_QUALITY_*.xlsx"))
        if not candidates:
            # Also try without batch suffix
            candidates = sorted(output_dir.glob("*_PAGE_QUALITY_*.xlsx"))
        if not candidates:
            raise FileNotFoundError(
                f"No PAGE_QUALITY report found in {output_dir}. "
                f"Run an ad-hoc extraction first or specify --report explicitly."
            )
        report_file = candidates[-1]

    print(f"\n  Using report: {report_file.name}")

    # ---- Load flagged rows -----------------------------------------------
    df = pd.read_excel(report_file)
    flagged = df[df['Flagged'] == True].copy()  # noqa: E712

    flagged_count = len(flagged)
    if flagged_count == 0:
        print("  ✓ No flagged pages — nothing to review.")
        return {'flagged_count': 0, 'pdfs_copied': 0, 'texts_copied': 0,
                'missing_pdfs': [], 'missing_texts': []}

    print(f"  ℹ {flagged_count} flagged page(s) to copy\n")

    # ---- Load config for PDF path + document source ----------------------
    pdf_source_path, doc_source = _load_config(plan_dir)

    # ---- Locate text files: search all batch subfolders ------------------
    documents_dir = plan_dir / "documents"
    text_lookup = _build_text_lookup(documents_dir)

    # ---- Copy files ------------------------------------------------------
    pdfs_copied    = 0
    texts_copied   = 0
    missing_pdfs   = []
    missing_texts  = []

    for _, row in flagged.iterrows():
        filename   = str(row.get('Filename', ''))
        ssn        = str(row.get('SSN', ''))
        docid      = str(row.get('DocID', ''))
        page_num   = str(row.get('Page Number', ''))

        # ---- Copy PDF ----------------------------------------------------
        if pdf_source_path and doc_source:
            pdf_name, pdf_src = _find_pdf(pdf_source_path, doc_source, ssn, docid, page_num)
            if pdf_src:
                dest = review_dir / pdf_name
                shutil.copy2(pdf_src, dest)
                pdfs_copied += 1
                logger.debug(f"Copied PDF: {pdf_name}")
            else:
                missing_pdfs.append(filename)
                logger.warning(f"PDF not found for: {filename}")
        else:
            logger.debug("PDF source path or document source not configured — skipping PDF copy")

        # ---- Copy text file ----------------------------------------------
        txt_src = text_lookup.get(filename)
        if txt_src:
            dest = text_dir / filename
            shutil.copy2(txt_src, dest)
            texts_copied += 1
            logger.debug(f"Copied text: {filename}")
        else:
            missing_texts.append(filename)
            logger.warning(f"Text file not found for: {filename}")

    # ---- Summary ---------------------------------------------------------
    print(f"  ✓ PDFs copied:   {pdfs_copied}")
    print(f"  ✓ Texts copied:  {texts_copied}")
    if missing_pdfs:
        print(f"  ⚠ PDFs not found ({len(missing_pdfs)}): {', '.join(missing_pdfs[:5])}"
              + (" ..." if len(missing_pdfs) > 5 else ""))
    if missing_texts:
        print(f"  ⚠ Texts not found ({len(missing_texts)}): {', '.join(missing_texts[:5])}"
              + (" ..." if len(missing_texts) > 5 else ""))
    print(f"\n  Review folder: {review_dir}\n")

    return {
        'flagged_count': flagged_count,
        'pdfs_copied':   pdfs_copied,
        'texts_copied':  texts_copied,
        'missing_pdfs':  missing_pdfs,
        'missing_texts': missing_texts,
    }


def list_reports(plan_name: str, plan_folder: str = None) -> None:
    """Print all PAGE_QUALITY reports available for a plan."""
    plan_dir   = Path(plan_folder) if plan_folder else Path("plans") / plan_name
    output_dir = plan_dir / "output"
    candidates = sorted(output_dir.glob("*_PAGE_QUALITY_*.xlsx"))

    if not candidates:
        print(f"  No PAGE_QUALITY reports found in {output_dir}")
        return

    print(f"\n  PAGE_QUALITY reports for '{plan_name}':")
    print(f"  {'#':<4} {'Filename':<60} {'Flagged'}")
    print("  " + "-" * 72)
    for i, f in enumerate(candidates, 1):
        try:
            df = pd.read_excel(f)
            n_flagged = int(df['Flagged'].sum()) if 'Flagged' in df.columns else '?'
        except Exception:
            n_flagged = '?'
        print(f"  {i:<4} {f.name:<60} {n_flagged}")
    print()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_config(plan_dir: Path) -> tuple:
    """
    Return (pdf_source_path, doc_source) from adhoc_config.toml and master config.
    Falls back gracefully if either is missing.
    """
    pdf_source_path = ''
    doc_source      = ''

    # Master config for pdf_source_path
    try:
        from config_loader import get_master_config
        master = get_master_config()
        pdf_source_path = master.pdf_source_path or ''
    except Exception as e:
        logger.debug(f"Could not load master config: {e}")

    # adhoc_config.toml for doc_source and optional pdf_folder override
    adhoc_config = plan_dir / "adhoc_config.toml"
    if adhoc_config.exists():
        try:
            import toml
            cfg = toml.load(adhoc_config)
            doc_source = cfg.get('Document', {}).get('document_source', '')
            # adhoc config can override the pdf folder via [Matching] pdf_folder
            matching_pdf = cfg.get('Matching', {}).get('pdf_folder', '')
            if matching_pdf:
                # Matching.pdf_folder points directly to the source folder (no subfolder)
                pdf_source_path = str(Path(matching_pdf).parent)
                doc_source = Path(matching_pdf).name or doc_source
        except Exception as e:
            logger.debug(f"Could not load adhoc_config.toml: {e}")

    return pdf_source_path, doc_source


def _find_pdf(pdf_source_path: str, doc_source: str, ssn: str, docid: str, page_num: str):
    """
    Locate the PDF for a given SSN/DocID/Page combination.

    PDF naming convention: 2_{SSN}_{DOCID}_{SOURCE}_Page_{N}.pdf
    PDFs live under: {pdf_source_path}/{doc_source}/

    Returns:
        (pdf_filename, full_path_as_Path) or ('', None) if not found.
    """
    if not (pdf_source_path and doc_source and ssn and docid):
        return '', None

    pdf_dir      = Path(pdf_source_path) / doc_source
    expected     = f"2_{ssn}_{docid}_{doc_source}_Page_{page_num}.pdf"
    expected_path = pdf_dir / expected

    if expected_path.exists():
        return expected, expected_path

    # Fallback: glob for any page number variant (handles zero-padding etc.)
    pattern = str(pdf_dir / f"2_{ssn}_{docid}_{doc_source}_Page_*.pdf")
    # Narrow to the right page number
    page_glob = _glob.glob(str(pdf_dir / f"2_{ssn}_{docid}_{doc_source}_Page_{page_num}.pdf"))
    if not page_glob:
        page_glob = _glob.glob(str(pdf_dir / f"2_{ssn}_{docid}_{doc_source}_Page_0{page_num}.pdf"))
    if page_glob:
        p = Path(page_glob[0])
        return p.name, p

    logger.debug(f"PDF not found: {expected} in {pdf_dir}")
    return '', None


def _build_text_lookup(documents_dir: Path) -> dict:
    """
    Walk all batch subfolders under documents/ and build a {filename: Path} map.
    Searches original files and cleaned/ subfolders (prefers original).
    """
    lookup = {}
    if not documents_dir.exists():
        return lookup

    for batch_dir in documents_dir.iterdir():
        if not batch_dir.is_dir() or batch_dir.name.startswith('.'):
            continue
        # Original files first
        for f in batch_dir.iterdir():
            if f.is_file() and not f.name.startswith('.'):
                lookup.setdefault(f.name, f)
        # Also index cleaned/ in case the original was removed
        cleaned = batch_dir / "cleaned"
        if cleaned.exists():
            for f in cleaned.iterdir():
                if f.is_file() and not f.name.startswith('.'):
                    lookup.setdefault(f.name, f)

    return lookup


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Copy flagged pages from a PAGE_QUALITY report into a review folder."
    )
    parser.add_argument('--plan',   required=True, help='Plan name (folder under plans/)')
    parser.add_argument('--report', default=None,  help='Explicit path to PAGE_QUALITY Excel file')
    parser.add_argument('--list',   action='store_true', help='List available PAGE_QUALITY reports and exit')
    args = parser.parse_args()

    if args.list:
        list_reports(args.plan)
        return

    run_review(plan_name=args.plan, report_path=args.report)


if __name__ == '__main__':
    main()
