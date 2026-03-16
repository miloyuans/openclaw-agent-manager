import sys
import os
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


def fill_first_available(page, selectors, value):
    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() > 0:
            locator.first.fill(value)
            return True
    return False


def click_first_available(page, selectors):
    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() > 0:
            locator.first.click()
            return True
    return False


def capture_token(login_url, username, password):
    mode = os.getenv("OPENCLAW_CAPTURE_MODE", "auto").lower()
    if mode == "headless":
        headless = True
    elif mode == "headful":
        headless = False
    else:
        if sys.platform.startswith("linux"):
            headless = not bool(os.getenv("DISPLAY") or os.getenv("WAYLAND_DISPLAY"))
        else:
            headless = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(login_url, wait_until="domcontentloaded", timeout=30_000)

        filled_user = fill_first_available(
            page,
            [
                'input[type="email"]',
                'input[name="email"]',
                'input[name="username"]',
                'input[type="text"]',
            ],
            username,
        )
        filled_pass = fill_first_available(
            page,
            ['input[type="password"]', 'input[name="password"]'],
            password,
        )
        clicked_submit = click_first_available(
            page,
            [
                'button[type="submit"]',
                'button:has-text("Log in")',
                'button:has-text("Sign in")',
                'button:has-text("登录")',
            ],
        )

        if not filled_user or not filled_pass or not clicked_submit:
            browser.close()
            print("NO_TOKEN_FOUND")
            return

        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except PlaywrightTimeoutError:
            page.wait_for_timeout(8_000)

        token = page.evaluate(
            """() => {
                const keys = ['token', 'authToken', 'accessToken', 'id_token', 'access_token'];
                for (const key of keys) {
                    if (localStorage.getItem(key)) return localStorage.getItem(key);
                    if (sessionStorage.getItem(key)) return sessionStorage.getItem(key);
                }
                for (let i = 0; i < localStorage.length; i++) {
                    const k = localStorage.key(i);
                    if (k && /token/i.test(k)) return localStorage.getItem(k);
                }
                for (let i = 0; i < sessionStorage.length; i++) {
                    const k = sessionStorage.key(i);
                    if (k && /token/i.test(k)) return sessionStorage.getItem(k);
                }
                return null;
            }"""
        )

        browser.close()
        print(token or "NO_TOKEN_FOUND")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("NO_TOKEN_FOUND")
        raise SystemExit(1)
    capture_token(sys.argv[1], sys.argv[2], sys.argv[3])
