# The fixture

`online_retail_fixture.csv` is 41 hand-written rows in the exact shape
`make data` produces from the UCI *Online Retail* workbook: same header, ISO
timestamps, empty string for a missing `CustomerID`. The test suite runs
against this file only, so it needs neither network nor the 23 MB download.

Every trap the real dataset carries is here at least once, with a customer
whose role in the tests is fixed. If you change a row, the expected numbers
in `tests/` change with it; the roles are what to preserve.

## Customers

| Customer | Role | What the tests assert |
|---|---|---|
| 10001 | Best customer: 5 invoices, Dec–Mar, one cancellation (`C600032`, −£25.50) | The cancellation lowers *this* customer's monetary value and still appears as negative revenue in March |
| 10002 | Bought £49.50 in January, cancelled all of it in February | Net spend is zero: not in RFM, does not found the January cohort. Both months still carry the amounts |
| 10003 | One invoice line appears twice, byte for byte | The duplicate is quarantined; frequency is 2, not 3, and monetary is not inflated |
| 10004, 10005, 10006 | Tied on frequency (2 invoices each), different spend; 10004 and 10005 also tie on recency | Tied customers get the same score, every run |
| 10007 | First purchase 31 Jan 23:55, second 1 Feb 00:10 | Cohort month index is 1, not 0 — months are calendar months, not 30-day windows |
| 10008 | Only a cancellation in the window (the purchase predates the data) | Net negative: excluded from RFM and cohorts; the −£51.00 is still in February revenue |
| 10009 | One purchase in December, never returned | The December cohort loses them at month index 1 |
| 10010 | One purchase, 3 days before the snapshot | Recency near zero with the lowest frequency: recent but unproven |
| 10011 | December, then February (skips January) | Retained at index 2 without being retained at index 1 |
| 10012 | December, January, February | Present at indexes 0, 1 and 2 |
| *(none)* | Four rows with no `CustomerID`, one of them a cancellation | Absent from RFM, present in monthly revenue: £51.00, £25.50 net, £165.00 |

## Rows that must land in the quarantine

| Row | Reason(s) |
|---|---|
| `600013` / `21755`, second copy | `duplicate` |
| `600001` / `22222` at £0 | `zero_unit_price` |
| `600002` / `POST`, `600010` / `M`, `600046` / `AMAZONFEE` | `non_product_stock_code` |
| `A600099` / `B` at −£1000 ("Adjust bad debt") | `negative_unit_price`, `non_product_stock_code` |
| `600050` / `23343`, −20 units at £0 ("damages") | `zero_unit_price`, `negative_quantity_outside_cancellation` |
| `600047` dated 30 February | `unparseable_invoice_date` |
| `600048` with quantity `ten` | `unparseable_quantity` |

The last two do not occur in the real file; they exist because bronze reads
everything as text on purpose, and silver has to say what it does with text
it cannot type.

## Expected totals

Snapshot date (max invoice date among valid rows): **2011-03-31**.

| Month | Net revenue | Identified | Unidentified | Orders | Customers |
|---|---|---|---|---|---|
| 2010-12 | 306.42 | 255.42 | 51.00 | 5 | 4 |
| 2011-01 | 438.02 | 438.02 | 0.00 | 8 | 8 |
| 2011-02 | 109.44 | 83.94 | 25.50 | 5 | 4 |
| 2011-03 | 639.88 | 474.88 | 165.00 | 8 | 6 |
| **Total** | **1493.76** | **1252.26** | **241.50** | | |

Ten customers are eligible for RFM (net spend > 0); their monetary values sum
to 1303.26 = identified revenue 1252.26 − (10002: 0.00) − (10008: −51.00).
