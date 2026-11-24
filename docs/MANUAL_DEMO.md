# CatalogIQ interview demo checklist

## Setup

- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Create `.env` from `.env.example` (`cp .env.example .env`; on PowerShell, use `Copy-Item .env.example .env`). Confirm `LLM_PROVIDER=mock` and `MOCK_FAILURE_RATE=0.1`.
- [ ] Start the app: `uvicorn app.main:app --reload`
- [ ] Open <http://localhost:8000>.

## Upload flow (time this run)

- [ ] Use the keyboard to focus the CSV file input and choose `data/sample.csv`.
- [ ] Select **Upload & Process**.
- [ ] Note the job ID and confirm the progress bar advances at least three times within 10 seconds.
- [ ] Wait until `done + failed = total`. Cache hits are included in `done`, so do not add `cache_hits` again.
- [ ] Confirm `cache_hits > 0` to demonstrate deduplication.
- [ ] Note the total processing time.

## Catalogue flow

- [ ] Open the Catalogue tab (Tab to the tab list, then Arrow Right).
- [ ] Type `amul` in Search Listings and wait for the 300ms debounce.
- [ ] Set Category to **Groceries** and confirm results narrow.
- [ ] Press Escape in the search field; confirm the query clears and results return from page 1.
- [ ] Focus a product row and press Enter to open Review.
- [ ] Return to Catalogue, select Next, and confirm page 2 loads when available.

## Review and approval

- [ ] Set Clean Title to `AMUL Butter 500 g (Pack of 2) — approved`.
- [ ] Set Category to **Groceries**.
- [ ] Select **Approve Product** and confirm the **Product approved** toast and approved status.
- [ ] Return to Catalogue and confirm the edited title appears.
- [ ] There is no Status filter in the current UI. Verify persistence by opening `/api/products/<sku>` or confirming the row's approved status.

## Failed product handling

- [ ] Stop the server and set `MOCK_FAILURE_RATE=1.0` in `.env`.
- [ ] Restart the server and upload a CSV; wait for retries to finish.
- [ ] Open a failed listing in Review and confirm its pipeline error is prominent.
- [ ] Restore `MOCK_FAILURE_RATE=0.1` and restart the server.

## Mobile check (360px)

- [ ] In DevTools, enable the device toolbar and set a custom viewport of 360×640 (or use iPhone SE at 375×667).
- [ ] Confirm the page body has no horizontal scrollbar.
- [ ] Inspect buttons and inputs; confirm touch targets are at least 44px tall.
- [ ] Confirm the upload progress and counts remain readable.
- [ ] Confirm the Catalogue table scrolls inside its own container.
- [ ] Confirm Raw and Cleaned review panels stack vertically.

## Keyboard-only pass

- [ ] Start at the address bar and Tab into the page; confirm visible focus rings.
- [ ] Move through the tab list, then the active panel controls. The current DOM order places the tabs before panel controls.
- [ ] Use Arrow Left/Right on the tab list to change tabs.
- [ ] In Catalogue, Tab through Search Listings, Category, product rows, then pagination.
- [ ] On a focused product row, verify both Enter and Space open the product.
- [ ] Focus Search Listings and press Escape; confirm the search clears.
