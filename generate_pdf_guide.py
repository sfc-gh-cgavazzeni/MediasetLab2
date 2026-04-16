#!/usr/bin/env python3
"""
Genera il PDF della Guida Partecipanti per il Workshop 2 Snowflake AI & Cortex - Mediaset.
Utilizza Chrome headless per stampare l'HTML in PDF con rendering accurato.
"""
import subprocess
import os
import sys
import shutil

# --- Paths ---
script_dir = os.path.dirname(os.path.abspath(__file__))
html_path = os.path.join(script_dir, "STUDENT_GUIDE.html")
output_path = os.path.join(script_dir, "STUDENT_GUIDE.pdf")

# --- Find Chrome ---
def find_chrome():
    """Find Chrome/Chromium executable on the system."""
    candidates = [
        # macOS
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        # Linux
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
        # Windows
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    ]
    
    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate
        found = shutil.which(candidate)
        if found:
            return found
    
    return None


def generate_pdf():
    chrome = find_chrome()
    if not chrome:
        print("ERRORE: Chrome/Chromium non trovato. Installa Google Chrome per generare il PDF.")
        sys.exit(1)

    if not os.path.isfile(html_path):
        print(f"ERRORE: File HTML non trovato: {html_path}")
        sys.exit(1)

    html_url = f"file://{html_path}"

    cmd = [
        chrome,
        "--headless",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-software-rasterizer",
        f"--print-to-pdf={output_path}",
        "--print-to-pdf-no-header",
        "--run-all-compositor-stages-before-draw",
        "--virtual-time-budget=5000",
        html_url,
    ]

    print(f"Generazione PDF con Chrome headless...")
    print(f"  Input:  {html_path}")
    print(f"  Output: {output_path}")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

    if result.returncode != 0:
        print(f"ERRORE Chrome (exit code {result.returncode}):")
        if result.stderr:
            print(result.stderr)
        sys.exit(1)

    if os.path.isfile(output_path):
        size_kb = os.path.getsize(output_path) / 1024
        print(f"PDF generato con successo: {output_path} ({size_kb:.0f} KB)")
    else:
        print("ERRORE: Il file PDF non e' stato creato.")
        sys.exit(1)


if __name__ == "__main__":
    generate_pdf()
