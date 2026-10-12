import os
import time

import requests
import streamlit as st

st.title("rail-pulse-poller: reachability test")

base_url = os.environ.get("BASE_URL", "")
token = os.environ.get("TOKEN", "")
path = st.text_input("Endpoint path", "")

st.write("BASE_URL set:", bool(base_url), "| TOKEN set:", bool(token))

if st.button("Call the API once"):
    started = time.time()
    try:
        r = requests.get(
            f"{base_url.rstrip('/')}/{path.lstrip('/')}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
        )
        st.write("HTTP status:", r.status_code, "| seconds:", round(time.time() - started, 2))
        st.code(r.text[:1500])
    except requests.RequestException as e:
        st.error(f"Request failed: {e!r}")
