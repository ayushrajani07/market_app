import calendar
from datetime import datetime, date
from typing import Any, Dict
import pytz
import time
import requests
from kiteconnect import KiteConnect
from kiteconnect.exceptions import KiteException

IST = pytz.timezone("Asia/Kolkata")

# Optional hooks (assign from outside)
on_quote_success = None     # def on_quote_success(latency_ms: float): ...
on_quote_429 = None         # def on_quote_429(): ...
on_quote_error = None       # def on_quote_error(): ...

def safe_call(kite, ensure_token_fn, method_name, *args, **kwargs):
    """
    Wrapper around Kite API calls to auto-refresh token on auth error
    and retry lightly on rate limiting (429 Too Many Requests).
    Also triggers optional broker health hooks for quote.
    """
    max_retries = 3
    base_delay = 0.2  # 200 ms
    attempt = 0

    while True:
        t0 = time.time()
        try:
            m = getattr(kite, method_name)
            out = m(*args, **kwargs)
            # success hook for quote
            if method_name == "quote" and callable(on_quote_success):
                try:
                    lat_ms = (time.time() - t0) * 1000.0
                    on_quote_success(latency_ms=lat_ms)
                except Exception:
                    pass
            return out

        except KiteException as ex:
            # 429 handling
            if "Too many requests" in str(ex) or getattr(ex, "code", None) == 429:
                if method_name == "quote" and callable(on_quote_429):
                    try:
                        on_quote_429()
                    except Exception:
                        pass
                retry_after = None
                if hasattr(ex, "response") and hasattr(ex.response, "headers"):
                    try:
                        retry_after = int(ex.response.headers.get("Retry-After", 0))
                    except Exception:
                        retry_after = None
                if retry_after and retry_after > 0:
                    delay = min(retry_after, 2)
                else:
                    delay = min(base_delay * (2 ** attempt), 2.0)
                time.sleep(delay)
                attempt += 1
                if attempt > max_retries:
                    if method_name == "quote" and callable(on_quote_error):
                        try:
                            on_quote_error()
                        except Exception:
                            pass
                    return {}
                continue

            # Auth issues -> refresh token once per attempt
            if "TokenException" in str(type(ex)) or "TokenException" in str(ex):
                try:
                    ensure_token_fn()
                except Exception:
                    pass
                attempt += 1
                if attempt > max_retries:
                    if method_name == "quote" and callable(on_quote_error):
                        try:
                            on_quote_error()
                        except Exception:
                            pass
                    return {}
                continue

            # Other Kite exception
            if method_name == "quote" and callable(on_quote_error):
                try:
                    on_quote_error()
                except Exception:
                    pass
            return {}

        except requests.exceptions.HTTPError as http_ex:
            # HTTP 429
            if http_ex.response is not None and http_ex.response.status_code == 429:
                if method_name == "quote" and callable(on_quote_429):
                    try:
                        on_quote_429()
                    except Exception:
                        pass
                retry_after = http_ex.response.headers.get("Retry-After")
                try:
                    retry_after = int(retry_after)
                except Exception:
                    retry_after = None
                delay = min(retry_after or base_delay * (2 ** attempt), 2.0)
                time.sleep(delay)
                attempt += 1
                if attempt > max_retries:
                    if method_name == "quote" and callable(on_quote_error):
                        try:
                            on_quote_error()
                        except Exception:
                            pass
                    return {}
                continue
            else:
                if method_name == "quote" and callable(on_quote_error):
                    try:
                        on_quote_error()
                    except Exception:
                        pass
                return {}

        except Exception:
            if method_name == "quote" and callable(on_quote_error):
                try:
                    on_quote_error()
                except Exception:
                    pass
            return {}

def safe_ltp(kite_client: KiteConnect, ensure_token_fn, tokens):
    return safe_call(kite_client, ensure_token_fn, "ltp", tokens) or {}

def get_now():
    return datetime.now(IST)

def parse_expiry(inst: Dict[str, Any]) -> date:
    e = inst["expiry"]
    return e if isinstance(e, date) else datetime.strptime(e, "%Y-%m-%d").date()

def this_month_expiry() -> date:
    t = date.today()
    cal = calendar.monthcalendar(t.year, t.month)
    thurs = [w[3] for w in cal if w[2] != 0]
    return date(t.year, t.month, thurs[-1])

def next_month_expiry() -> date:
    t = date.today()
    m = (t.month % 12) + 1
    y = t.year + (t.month == 12)
    cal = calendar.monthcalendar(y, m)
    thurs = [w[2] for w in cal if w[2] != 0]
    return date(y, m, thurs[-1])
