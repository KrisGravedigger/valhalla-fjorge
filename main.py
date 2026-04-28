#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Valhalla Fjorge - pipeline entry point shim.
All logic lives in valhalla/pipeline/.
"""

import io
import sys

# Force UTF-8 output on Windows (must be BEFORE any print() or import that prints).
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from valhalla.pipeline import main

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n\n[main] Przerwano przez uzytkownika.')
        sys.exit(130)
