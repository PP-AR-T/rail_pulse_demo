import json
import threading

import poller
import streamlit as st

st.set_page_config(page_title="rail-pulse-poller")
st.title("rail-pulse-poller")
st.caption("Data: Realtime Trains (realtimetrains.co.uk). Personal, non-commercial use.")


@st.cache_resource
def start_poller():
    """Start the polling thread once per app process."""
    status = {"files_written": 0}
    cache = poller.TokenCache(poller.BASE_URL, poller.REFRESH_TOKEN)
    thread = threading.Thread(
        target=poller.run_forever,
        args=(threading.Event(), status, cache, poller.VolumeWriter()),
        daemon=True,
    )
    thread.start()
    return status


try:
    status = start_poller()
except Exception as e:
    st.error(f"Poller did not start: {e!r}")
    st.stop()

st.write(f"Polling {poller.STATION} every {poller.INTERVAL_S} s into `{poller.LANDING_DIR}`")
st.button("Refresh view (does not call the API)")
st.write("Last call (UTC):", status.get("last_call"))
st.write("Last HTTP status:", status.get("last_status"))
st.write("Files written since start:", status.get("files_written"))
st.write("Last file:", status.get("last_path"))
if status.get("last_error"):
    st.error(status["last_error"])

body = status.get("last_body")
if body:
    try:
        st.subheader("Latest response (expand to explore)")
        st.json(json.loads(body), expanded=1)
    except json.JSONDecodeError:
        st.code(body[:1500])
