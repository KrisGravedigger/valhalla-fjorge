"""Recover insufficient_balance events from archive files into CSV."""

from pathlib import Path


def recover_insufficient_balance_history(output_dir: str) -> None:
    """Scan archive files and recover all insufficient_balance events into CSV."""
    from valhalla.models import extract_date_from_filename
    from valhalla.readers import PlainTextReader, HtmlReader, detect_input_format
    from valhalla.event_parser import EventParser
    from valhalla.csv_writer import CsvWriter

    archive_dir = Path('archive')
    if not archive_dir.exists():
        print("No archive/ directory found")
        return

    archive_files = [f for f in archive_dir.iterdir()
                     if f.is_file() and f.suffix in ('.txt', '.html')]

    if not archive_files:
        print("No .txt or .html files in archive/")
        return

    print(f"Scanning {len(archive_files)} archive file(s) for insufficient balance events...")

    all_events = []
    for filepath in sorted(archive_files):
        fmt = detect_input_format(str(filepath))
        if fmt == 'html':
            reader = HtmlReader(str(filepath))
        else:
            reader = PlainTextReader(str(filepath))

        messages = reader.read()

        # Detect date for this file
        file_date = extract_date_from_filename(str(filepath))
        if not file_date and reader.header_date:
            file_date = reader.header_date

        # Check for embedded timestamps
        has_full_timestamps = any(
            '[' in msg.timestamp and 'T' in msg.timestamp and len(msg.timestamp) > 7
            for msg in messages
        )
        if has_full_timestamps:
            file_date = None  # dates embedded

        file_parser = EventParser(base_date=file_date)
        file_parser.parse_messages(messages)

        if file_parser.insufficient_balance_events:
            all_events.extend(file_parser.insufficient_balance_events)
            print(f"  {filepath.name}: {len(file_parser.insufficient_balance_events)} event(s)")

    if all_events:
        insuf_csv = Path(output_dir) / 'insufficient_balance.csv'
        csv_writer = CsvWriter()
        csv_writer.generate_insufficient_balance_csv(all_events, str(insuf_csv))
        print(f"\nRecovered {len(all_events)} insufficient balance events -> {insuf_csv}")
    else:
        print("No insufficient balance events found in archive files")
