"""
copy_lifecycle.py - Copy lifecycle management files to account-specific destinations

Purpose:
    Copies the lifecycle Excel files from the year-month source folder
    to account-specific destination folders (e.g., SharePoint sync folder, network drive).
    Supports mapping account names to different destination folder names.

Configuration (in config/settings.json):
    "lifecycle_copy": {
        "source_path": "output/life_cycle_mgmt/{YYYY}-{MM}",
        "destination_base": "/path/to/BC-Video-Dump/DATA",
        "destination_filename": "{ACCOUNT}_cms.xlsx",
        "file_pattern": "*_cms.xlsx",
        "account_folder_mapping": {
            "circleone": "Circle-1--GWM",
            "Internet": "IntERnet-Shared",
            "Intranet": "IntRAnet--Shared"
        }
    }

    Placeholders supported:
        {MM}            - Current month (01-12)
        {YYYY}          - Current year (2024, 2025, etc.)
        {DD}            - Current day (01-31)
        {ACCOUNT}       - Account name extracted from filename
        {MAPPED_FOLDER} - Mapped destination folder name from account_folder_mapping
        {YYYY}_{MM}     - Year_month with underscore (e.g., 2026_01)
        {YYYY}-{MM}     - Year-month with hyphen (e.g., 2026-01)

Usage:
    python copy_lifecycle.py

    Optional flags:
        --dry-run       Show what would be copied without copying
        --destination   Override base destination path from command line
        --year          Override year (e.g., --year 2025)
        --month         Override month (e.g., --month 01 for January)

Examples:
    # Copy current year-month's files to configured destinations
    python copy_lifecycle.py

    # Preview what would be copied
    python copy_lifecycle.py --dry-run

    # Copy a specific year-month's files
    python copy_lifecycle.py --year 2025 --month 12

Example destination structure:
    With destination_base = "/sharepoint/BC-Video-Dump/DATA"
    And account_folder_mapping = {"Internet": "IntERnet-Shared", "neo": "NEO--IB"}

    Internet_cms.xlsx  -> /sharepoint/BC-Video-Dump/DATA/IntERnet-Shared/2026_01/Internet_cms.xlsx
    neo_cms.xlsx       -> /sharepoint/BC-Video-Dump/DATA/NEO--IB/2026_01/neo_cms.xlsx
"""

import sys
import re
import shutil
import argparse
from pathlib import Path
from datetime import datetime

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from shared import (
    load_config,
    setup_logging,
    get_output_paths,
)

SCRIPT_NAME = "copy_lifecycle"


def extract_account_from_filename(filename: str) -> str:
    """
    Extract account name from filename.

    Examples:
        "Internet_cms.xlsx" -> "Internet"
        "research_internal_cms.xlsx" -> "research_internal"
        "MyWay_cms.xlsx" -> "MyWay"

    Args:
        filename: The filename to parse

    Returns:
        Account name or None if pattern doesn't match
    """
    # Pattern: {account}_cms.xlsx
    match = re.match(r'^(.+)_cms\.xlsx$', filename, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def expand_placeholders(
    path_template: str,
    year_override: str = None,
    month_override: str = None,
    account: str = None,
    mapped_folder: str = None
) -> str:
    """
    Expand placeholders in path template.

    Supported placeholders:
        {MM}           - Current month (01-12)
        {YYYY}         - Current year
        {DD}           - Current day
        {ACCOUNT}      - Account name (from filename)
        {MAPPED_FOLDER} - Mapped destination folder name
        {YYYY}_{MM}    - Year_month with underscore (e.g., 2026_01)
        {YYYY}-{MM}    - Year-month with hyphen (e.g., 2026-01)

    Args:
        path_template: Path string with placeholders
        year_override: Optional year override (e.g., "2025")
        month_override: Optional month override (01-12)
        account: Optional account name for {ACCOUNT} placeholder
        mapped_folder: Optional mapped folder name for {MAPPED_FOLDER} placeholder

    Returns:
        Expanded path string
    """
    now = datetime.now()

    year = year_override if year_override else now.strftime("%Y")
    month = month_override if month_override else now.strftime("%m")

    replacements = {
        "{MM}": month,
        "{YYYY}": year,
        "{DD}": now.strftime("%d"),
        "{YYYY}_{MM}": f"{year}_{month}",
        "{YYYY}-{MM}": f"{year}-{month}",
    }

    if account:
        replacements["{ACCOUNT}"] = account

    if mapped_folder:
        replacements["{MAPPED_FOLDER}"] = mapped_folder

    result = path_template
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)

    return result


def copy_files_to_account_destinations(
    source_dir: Path,
    destination_template: str,
    filename_template: str,
    file_pattern: str,
    account_folder_mapping: dict,
    year_override: str,
    month_override: str,
    dry_run: bool,
    logger
) -> int:
    """
    Copy files to account-specific destination folders.

    Args:
        source_dir: Source directory containing files
        destination_template: Destination path template with {MAPPED_FOLDER} placeholder
        filename_template: Destination filename template (e.g., "{ACCOUNT}_cms.xlsx")
        file_pattern: Glob pattern for files to copy (e.g., "*_cms.xlsx")
        account_folder_mapping: Dict mapping account names to destination folder names
        year_override: Optional year override
        month_override: Optional month override
        dry_run: If True, only log what would be done
        logger: Logger instance

    Returns:
        Number of files copied
    """
    if not source_dir.exists():
        logger.error(f"Source directory does not exist: {source_dir}")
        return 0

    files = list(source_dir.glob(file_pattern))

    if not files:
        logger.warning(f"No files matching '{file_pattern}' found in {source_dir}")
        return 0

    logger.info(f"Found {len(files)} files to copy")
    logger.info("-" * 60)

    copied = 0
    skipped = 0
    for source_file in files:
        # Extract account name from filename
        account = extract_account_from_filename(source_file.name)

        if not account:
            logger.warning(f"  Skipping {source_file.name} - cannot extract account name")
            skipped += 1
            continue

        # Look up mapped folder name (case-insensitive lookup)
        mapped_folder = None
        for key, value in account_folder_mapping.items():
            if key.lower() == account.lower():
                mapped_folder = value
                break

        if not mapped_folder:
            logger.warning(f"  Skipping {account} - no folder mapping configured")
            logger.warning(f"    Add to settings.json: \"{account}\": \"DESTINATION_FOLDER_NAME\"")
            skipped += 1
            continue

        # Expand destination path with mapped folder
        dest_dir = Path(expand_placeholders(
            destination_template,
            year_override,
            month_override,
            account,
            mapped_folder
        ))

        # Expand destination filename
        dest_filename = expand_placeholders(
            filename_template,
            year_override,
            month_override,
            account,
            mapped_folder
        )

        dest_file = dest_dir / dest_filename

        if dry_run:
            logger.info(f"  [DRY-RUN] {account} -> {mapped_folder}:")
            logger.info(f"    From: {source_file}")
            logger.info(f"    To:   {dest_file}")
        else:
            try:
                dest_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_file, dest_file)
                logger.info(f"  Copied: {account} -> {dest_file}")
                copied += 1
            except Exception as e:
                logger.error(f"  Failed to copy {account}: {e}")

    if skipped > 0:
        logger.info(f"Skipped {skipped} files (no mapping or invalid)")

    return copied if not dry_run else len(files) - skipped


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Copy lifecycle management files to account-specific destinations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Copies lifecycle Excel files to account-specific destination folders.
Supports mapping account names to different destination folder names.

Configure in config/settings.json:
    "lifecycle_copy": {
        "source_path": "output/life_cycle_mgmt/{YYYY}-{MM}",
        "destination_base": "/path/to/BC-Video-Dump/DATA",
        "destination_filename": "{ACCOUNT}_cms.xlsx",
        "file_pattern": "*_cms.xlsx",
        "account_folder_mapping": {
            "Internet": "IntERnet-Shared",
            "neo": "NEO--IB"
        }
    }

Placeholders:
    {YYYY}          - Current year
    {MM}            - Current month (01-12)
    {DD}            - Current day
    {ACCOUNT}       - Account name from filename
    {MAPPED_FOLDER} - Mapped destination folder name

Examples:
    python copy_lifecycle.py                          # Copy current year-month
    python copy_lifecycle.py --dry-run                # Preview only
    python copy_lifecycle.py --month 01               # Copy January (current year)
    python copy_lifecycle.py --year 2025 --month 12   # Copy Dec 2025

Example result (with mapping Internet -> IntERnet-Shared):
    Internet_cms.xlsx  -> /destination/IntERnet-Shared/2026_01/Internet_cms.xlsx
    neo_cms.xlsx       -> /destination/NEO--IB/2026_01/neo_cms.xlsx
        """
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be copied without actually copying'
    )
    parser.add_argument(
        '--destination',
        type=str,
        help='Override destination path template from settings'
    )
    parser.add_argument(
        '--year',
        type=str,
        help='Override year (e.g., 2025) instead of current year'
    )
    parser.add_argument(
        '--month',
        type=str,
        help='Override month (01-12) instead of current month'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting lifecycle file copy")
    logger.info("=" * 60)

    # Load configuration
    config = load_config()
    settings = config['settings']

    # Get lifecycle_copy settings
    lifecycle_config = settings.get('lifecycle_copy', {})

    if not lifecycle_config:
        logger.error("No 'lifecycle_copy' configuration found in settings.json")
        logger.error("Please add:")
        logger.error('  "lifecycle_copy": {')
        logger.error('    "source_path": "output/life_cycle_mgmt/{YYYY}-{MM}",')
        logger.error('    "destination_path": "/your/path/{ACCOUNT}/{YYYY}-{MM}",')
        logger.error('    "destination_filename": "{ACCOUNT}_cms.xlsx",')
        logger.error('    "file_pattern": "*_cms.xlsx"')
        logger.error('  }')
        return

    # Get source path (relative to project root)
    source_template = lifecycle_config.get('source_path', 'output/life_cycle_mgmt/{YYYY}-{MM}')
    source_expanded = expand_placeholders(source_template, args.year, args.month)

    # Make source path absolute (relative to project root)
    if not Path(source_expanded).is_absolute():
        source_path = paths['root'] / source_expanded
    else:
        source_path = Path(source_expanded)

    # Get destination base path and build template
    if args.destination:
        destination_base = args.destination
    else:
        destination_base = lifecycle_config.get('destination_base', '')
        if not destination_base:
            logger.error("No destination_base configured in settings.json")
            logger.error("Either set 'destination_base' in settings.json or use --destination flag")
            logger.error("Example: /sharepoint/BC-Video-Dump/DATA")
            return

    # Build destination template: base/{MAPPED_FOLDER}/{YYYY}_{MM}
    destination_template = f"{destination_base}/{{MAPPED_FOLDER}}/{{YYYY}}_{{MM}}"

    # Get account folder mapping
    account_folder_mapping = lifecycle_config.get('account_folder_mapping', {})
    if not account_folder_mapping:
        logger.error("No 'account_folder_mapping' configured in settings.json")
        logger.error("Please add account-to-folder mappings:")
        logger.error('  "account_folder_mapping": {')
        logger.error('    "Internet": "IntERnet-Shared",')
        logger.error('    "Intranet": "IntRAnet--Shared"')
        logger.error('  }')
        return

    # Get destination filename template
    filename_template = lifecycle_config.get('destination_filename', '{ACCOUNT}_cms.xlsx')

    # Get file pattern
    file_pattern = lifecycle_config.get('file_pattern', '*_cms.xlsx')

    # Log configuration
    current_year = args.year if args.year else datetime.now().strftime("%Y")
    current_month = args.month if args.month else datetime.now().strftime("%m")
    month_name = datetime.strptime(current_month, "%m").strftime("%B")

    logger.info(f"Period: {current_year}-{current_month} ({month_name} {current_year})")
    logger.info(f"Source: {source_path}")
    logger.info(f"Destination base: {destination_base}")
    logger.info(f"Destination template: {destination_template}")
    logger.info(f"Account mappings: {len(account_folder_mapping)} configured")
    logger.info(f"Filename template: {filename_template}")
    logger.info(f"Pattern: {file_pattern}")

    if args.dry_run:
        logger.info("Mode: DRY-RUN (no files will be copied)")
    else:
        logger.info("Mode: COPY")

    logger.info("")

    # Copy files
    copied = copy_files_to_account_destinations(
        source_dir=source_path,
        destination_template=destination_template,
        filename_template=filename_template,
        file_pattern=file_pattern,
        account_folder_mapping=account_folder_mapping,
        year_override=args.year,
        month_override=args.month,
        dry_run=args.dry_run,
        logger=logger
    )

    # Summary
    logger.info("-" * 60)
    if args.dry_run:
        logger.info(f"Would copy {copied} files")
    else:
        logger.info(f"Copied {copied} files")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()
