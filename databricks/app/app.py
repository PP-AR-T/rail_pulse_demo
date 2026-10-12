import os

import requests
import streamlit as st

BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")
REFRESH_TOKEN = os.environ.get("TOKEN", "")

st.title("rail-pulse-poller: RTT test")
st.write("BASE_URL set:", bool(BASE_URL), "| TOKEN set:", bool(REFRESH_TOKEN))

code = st.text_input("Station code", "BMH")
filter_to = st.text_input("Only trains calling at (optional)", "WAT")
window = st.number_input("Window (minutes)", 10, 180, 60)


def rate_headers(r):
    return {k: v for k, v in r.headers.items()
            if k.lower().startswith("x-ratelimit") or k.lower() == "retry-after"}


if st.button("Get access token and fetch departures"):
    try:
        t = requests.get(f"{BASE_URL}/api/get_access_token",
                         headers={"Authorization": f"Bearer {REFRESH_TOKEN}"}, timeout=20)
        st.write("Token exchange status:", t.status_code)
        if t.status_code != 200:
            st.code(t.text[:500])
            st.stop()
        info = t.json()
        st.write("Access token valid until:", info.get("validUntil"))
        params = {"code": code, "timeWindow": int(window)}
        if filter_to:
            params["filterTo"] = filter_to
        r = requests.get(f"{BASE_URL}/gb-nr/location", params=params,
                         headers={"Authorization": f"Bearer {info['token']}"}, timeout=20)
        st.write("Location status:", r.status_code)
        st.json(rate_headers(r))
        if r.status_code == 200:
            st.write("Services returned:", len(r.json().get("services", [])))
        st.code(r.text[:1500])
    except requests.RequestException as e:
        st.error(f"Request failed: {e!r}")
