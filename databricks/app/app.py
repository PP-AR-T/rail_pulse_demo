import json
import os

import requests
import streamlit as st

BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")
REFRESH_TOKEN = os.environ.get("TOKEN", "")

st.title("rail-pulse-poller: RTT explorer")
st.caption("Data: Realtime Trains (realtimetrains.co.uk). Personal, non-commercial use.")
st.write("BASE_URL set:", bool(BASE_URL), "| TOKEN set:", bool(REFRESH_TOKEN))

code = st.text_input("Station code", "BMH")
filter_to = st.text_input("Only trains calling at (optional)", "WAT")
window = st.number_input("Window (minutes)", 10, 180, 60)


def rate_headers(r):
    return {k: v for k, v in r.headers.items()
            if k.lower().startswith("x-ratelimit") or k.lower() == "retry-after"}


if st.button("Get access token and fetch departures"):
    out = {}
    try:
        t = requests.get(f"{BASE_URL}/api/get_access_token",
                         headers={"Authorization": f"Bearer {REFRESH_TOKEN}"}, timeout=20)
        out["token_status"] = t.status_code
        if t.status_code == 200:
            info = t.json()
            out["valid_until"] = info.get("validUntil")
            params = {"code": code, "timeWindow": int(window)}
            if filter_to:
                params["filterTo"] = filter_to
            r = requests.get(f"{BASE_URL}/gb-nr/location", params=params,
                             headers={"Authorization": f"Bearer {info['token']}"}, timeout=20)
            out.update(status=r.status_code, rate=rate_headers(r), text=r.text)
        else:
            out["text"] = t.text[:500]
    except requests.RequestException as e:
        out["error"] = repr(e)
    st.session_state["last"] = out

last = st.session_state.get("last")
if last:
    st.write("Token exchange status:", last.get("token_status"),
             "| valid until:", last.get("valid_until"))
    if "error" in last:
        st.error(last["error"])
    st.write("Location status:", last.get("status"))
    if last.get("rate"):
        st.json(last["rate"], expanded=False)
    text = last.get("text", "")
    try:
        data = json.loads(text) if text else None
    except json.JSONDecodeError:
        data = None
    if data is not None:
        st.write("Services returned:", len(data.get("services", [])))
        st.subheader("Response (click to expand)")
        st.json(data, expanded=2)
        st.download_button("Download raw JSON", text, file_name=f"rtt_{code}.json",
                           mime="application/json")
    elif text:
        st.code(text[:1500])
