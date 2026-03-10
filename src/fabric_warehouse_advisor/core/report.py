"""
Fabric Warehouse Advisor — Core Report Utilities
===================================================
Shared report-writing utilities used by all advisor modules.

Individual advisor modules generate their own report content in text,
Markdown, and HTML formats.  This module provides common helpers for
saving that content to disk.
"""

from __future__ import annotations

from pathlib import Path

# Number of leading characters to inspect when detecting whether
# content already contains an HTML document wrapper.
_HTML_CHECK_PREFIX_LEN = 200


def save_report(
    content: str,
    path: str,
    *,
    format: str | None = None,
) -> str:
    """Write report content to a file and return the absolute path.

    Parameters
    ----------
    content : str
        The report string (text, markdown, or HTML).
    path : str
        Destination path.  For Lakehouse files in Fabric use something
        like ``/lakehouse/default/Files/reports/advisor_report.html``.
    format : str, optional
        ``"html"``, ``"md"``, or ``"txt"``.  When *None* the format is
        inferred from *path*'s extension.

    Returns
    -------
    str
        The absolute path that was written.
    """
    p = Path(path)

    if format is None:
        ext = p.suffix.lower()
        if ext in (".html", ".htm"):
            format = "html"
        elif ext in (".md", ".markdown"):
            format = "md"
        else:
            format = "txt"

    # For HTML format, wrap in a minimal HTML document if not already present
    if format == "html" and "<html" not in content[:_HTML_CHECK_PREFIX_LEN].lower():
        content = (
            "<!DOCTYPE html>\n<html lang='en'>\n<head>"
            "<meta charset='utf-8'>"
            "<title>Fabric Warehouse Advisor Report</title>"
            "</head>\n<body>\n"
            + content
            + "\n</body>\n</html>"
        )

    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return str(p.resolve())
