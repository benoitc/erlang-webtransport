#!/usr/bin/env python3
"""
WebTransport Chrome Interop Test Automation

This script uses Selenium to run WebTransport interop tests in Chrome
against the Erlang WebTransport server.

Usage:
    python interop.py [--server-url URL] [--headless]
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def get_chrome_options(headless: bool = False) -> Options:
    """Configure Chrome options for WebTransport testing."""
    options = Options()

    # Required flags for WebTransport testing
    options.add_argument("--enable-features=WebTransportDeveloperMode")
    options.add_argument("--enable-quic")
    options.add_argument("--quic-version=h3")

    # Ignore certificate errors for self-signed certs
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-insecure-localhost")

    # Additional flags
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    if headless:
        options.add_argument("--headless=new")

    return options


def wait_for_server(url: str, timeout: int = 30) -> bool:
    """Wait for the server to be ready."""
    import socket
    from urllib.parse import urlparse

    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 443

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return True
        except Exception:
            pass
        time.sleep(1)

    return False


def run_tests(
    server_url: str = "https://localhost:4433/test",
    page_url: str = "http://localhost:8080/index.html",
    headless: bool = False,
) -> dict:
    """Run all WebTransport interop tests."""
    results = {"tests": [], "passed": 0, "failed": 0}

    print(f"Starting Chrome WebTransport interop tests")
    print(f"  Server URL: {server_url}")
    print(f"  Page URL: {page_url}")
    print(f"  Headless: {headless}")
    print()

    options = get_chrome_options(headless)
    driver = webdriver.Chrome(options=options)

    try:
        # Load the test page
        print("Loading test page...")
        driver.get(page_url)

        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "serverUrl"))
        )

        # Set the server URL
        server_input = driver.find_element(By.ID, "serverUrl")
        server_input.clear()
        server_input.send_keys(server_url)

        # Run all tests
        print("Running tests...")
        run_all_btn = driver.find_element(By.XPATH, "//button[text()='Run All Tests']")
        run_all_btn.click()

        # Wait for tests to complete
        time.sleep(10)

        # Collect results
        results_div = driver.find_element(By.ID, "results")
        test_results = results_div.find_elements(By.CLASS_NAME, "test-result")

        for result_el in test_results:
            class_list = result_el.get_attribute("class")
            text = result_el.text

            test_name = text.split(":")[0] if ":" in text else text

            if "pass" in class_list:
                status = "PASS"
                results["passed"] += 1
            elif "fail" in class_list:
                status = "FAIL"
                results["failed"] += 1
            else:
                status = "UNKNOWN"

            results["tests"].append(
                {"name": test_name, "status": status, "message": text}
            )

            print(f"  {status}: {text}")

        # Get log output
        log_div = driver.find_element(By.ID, "log")
        results["log"] = log_div.text

    except Exception as e:
        print(f"Error running tests: {e}")
        results["error"] = str(e)
        results["failed"] += 1

    finally:
        driver.quit()

    return results


def main():
    parser = argparse.ArgumentParser(description="WebTransport Chrome Interop Tests")
    parser.add_argument(
        "--server-url",
        default="https://localhost:4433/test",
        help="WebTransport server URL",
    )
    parser.add_argument(
        "--page-url",
        default="http://localhost:8080/index.html",
        help="Test page URL",
    )
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument(
        "--wait-for-server", action="store_true", help="Wait for server to be ready"
    )
    parser.add_argument("--json", action="store_true", help="Output results as JSON")

    args = parser.parse_args()

    if args.wait_for_server:
        print("Waiting for server...")
        if not wait_for_server(args.server_url):
            print("Server not ready, exiting")
            sys.exit(1)
        print("Server is ready")

    results = run_tests(
        server_url=args.server_url, page_url=args.page_url, headless=args.headless
    )

    print()
    print(f"Results: {results['passed']} passed, {results['failed']} failed")

    if args.json:
        print(json.dumps(results, indent=2))

    sys.exit(0 if results["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
