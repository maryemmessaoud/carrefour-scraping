from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # headless=False pour observer d'abord
    page = browser.new_page()
    page.goto("https://www.carrefour.fr/r/smartphones-objets-connectes")
    page.wait_for_timeout(5000)  # laisse la page charger
    print(page.title())
    content = page.content()
    with open("playwright_test.html", "w", encoding="utf-8") as f:
        f.write(content)
    browser.close()