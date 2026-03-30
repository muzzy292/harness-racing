from __future__ import annotations

import csv
import io
import sqlite3
from pathlib import Path

import streamlit as st

from harness_model.compare import compare_meeting_map_to_results, compare_meeting_scores_to_results, load_results_for_meeting
from harness_model.odds import (
    flatten_meeting_scores,
    load_feature_rows,
    score_meeting_rows,
    write_scored_rows_csv,
)
from harness_model.pipeline import build_feature_dataset, refresh_meeting


DEFAULT_MEETING_CODE = "LM300326"
DEFAULT_TRACK_PARS = r"C:\Users\Paul Mustica\Desktop\track_pars.json"
DEFAULT_RAW_DIR = "data/raw"
DEFAULT_DB = "data/harness.db"
DEFAULT_FEATURES = "data/features/runner_features.csv"
DEFAULT_ODDS_DIR = "data/odds"


st.set_page_config(page_title="Harness Odds Model", layout="wide")


def _init_state() -> None:
    st.session_state.setdefault("last_refresh_result", None)
    st.session_state.setdefault("last_score_rows", None)
    st.session_state.setdefault("last_scored_meeting", None)
    st.session_state.setdefault("last_comparison", None)
    st.session_state.setdefault("last_map_comparison", None)


def _csv_bytes(rows: list[dict[str, object]]) -> bytes:
    if not rows:
        return b""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def _race_feature_rows(rows: list[dict[str, str]], meeting_code: str, race_number: int) -> list[dict[str, str]]:
    return [
        row for row in rows
        if row.get("meeting_code") == meeting_code and int(row.get("race_number", 0) or 0) == race_number
    ]


def _show_status() -> None:
    refresh_result = st.session_state.get("last_refresh_result")
    if refresh_result:
        st.success(
            "Meeting refreshed.\n"
            f"HTML: {refresh_result['meeting_path']}\n"
            f"Features: {refresh_result['feature_path']}\n"
            f"Runners: {refresh_result['runner_count']}"
        )


def _render_scored_race_table(scored_rows: list[dict[str, object]]) -> None:
    display_rows = sorted(
        scored_rows,
        key=lambda row: (
            row.get("runner_number") is None,
            row.get("runner_number") if row.get("runner_number") is not None else 999,
        ),
    )
    table_rows = [
        {
            "No.": row.get("runner_number"),
            "Horse": row.get("horse_name"),
            "Barrier": row.get("barrier"),
            "Driver": row.get("nominated_driver"),
            "Trainer": row.get("nominated_trainer"),
            "Prob %": round(float(row.get("win_probability") or 0.0) * 100, 2),
            "Fair Odds": row.get("fair_odds"),
            "S1": row.get("stage1_score"),
            "S2": row.get("stage2_score"),
            "Score": row.get("score"),
        }
        for row in display_rows
    ]
    st.dataframe(table_rows, use_container_width=True, hide_index=True)


def _load_comparison(db_path: str, meeting_code: str, meeting_scores: dict[int, list[dict[str, object]]]) -> dict[str, object] | None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    result_rows = load_results_for_meeting(conn, meeting_code)
    conn.close()
    if not result_rows:
        return None
    comparison_rows, summary = compare_meeting_scores_to_results(meeting_code, meeting_scores, result_rows)
    return {"rows": comparison_rows, "summary": summary}


def _load_map_comparison(db_path: str, meeting_code: str, feature_rows: list[dict[str, str]]) -> dict[str, object] | None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    result_rows = load_results_for_meeting(conn, meeting_code)
    conn.close()
    if not result_rows:
        return None
    comparison_rows, summary = compare_meeting_map_to_results(meeting_code, feature_rows, result_rows)
    return {"rows": comparison_rows, "summary": summary}


def _render_results_audit(meeting_code: str) -> None:
    comparison_payload = st.session_state.get("last_comparison")
    if comparison_payload and comparison_payload.get("rows"):
        summary = comparison_payload["summary"]
        c1, c2, c3 = st.columns(3)
        c1.metric("Races compared", summary.get("races_compared"))
        c2.metric("Top-pick winners", summary.get("top_pick_winners"))
        c3.metric("Average winner rank", summary.get("avg_winner_rank"))
        with st.expander("Results comparison", expanded=True):
            st.dataframe(comparison_payload["rows"], use_container_width=True, hide_index=True)
            st.download_button(
                "Download results comparison CSV",
                data=_csv_bytes(comparison_payload["rows"]),
                file_name=f"{meeting_code}_results_comparison.csv",
                mime="text/csv",
                use_container_width=True,
            )


def _render_map_audit(meeting_code: str) -> None:
    map_comparison_payload = st.session_state.get("last_map_comparison")
    if map_comparison_payload and map_comparison_payload.get("rows"):
        summary = map_comparison_payload["summary"]
        m1, m2, m3 = st.columns(3)
        m1.metric("Map runners compared", summary.get("runners_compared"))
        m2.metric("Map actual tags", summary.get("runners_with_actual_tag"))
        m3.metric("Map match rate", summary.get("map_match_rate"))
        sorted_rows = sorted(
            map_comparison_payload["rows"],
            key=lambda row: (
                int(row.get("race_number") or 0),
                _barrier_sort_key(row.get("barrier")),
                str(row.get("horse_name") or "").upper(),
            ),
        )
        display_rows = [
            {
                "Race": row.get("race_number"),
                "Barrier": row.get("barrier"),
                "Horse": row.get("horse_name"),
                "Finish": row.get("finish_position"),
                "Pred Pos": row.get("predicted_map_position"),
                "Pred Bkt": row.get("predicted_map_tag"),
                "Conf": row.get("predicted_map_confidence"),
                "Actual Pos": row.get("actual_map_position"),
                "Actual Bkt": row.get("actual_map_tag"),
                "Match": row.get("map_match"),
                "Steward Comment": row.get("steward_comment"),
            }
            for row in sorted_rows
        ]
        with st.expander("Map comparison", expanded=True):
            st.dataframe(display_rows, use_container_width=True, hide_index=True)
            st.download_button(
                "Download map comparison CSV",
                data=_csv_bytes(sorted_rows),
                file_name=f"{meeting_code}_map_comparison.csv",
                mime="text/csv",
                use_container_width=True,
            )


def _barrier_sort_key(barrier: object) -> tuple[int, int]:
    text = str(barrier or "").strip().upper()
    if not text:
        return (9, 999)
    if text.startswith("FR"):
        try:
            return (0, int(text[2:]))
        except ValueError:
            return (0, 999)
    if text.startswith("SR"):
        try:
            return (1, int(text[2:]))
        except ValueError:
            return (1, 999)
    return (8, 999)


def main() -> None:
    _init_state()

    st.title("Harness Racing Odds Model")
    st.caption("Refresh meetings, rebuild features, and score races from a local control panel.")

    with st.sidebar:
        st.header("Meeting Inputs")
        meeting_code = st.text_input("Meeting code", value=DEFAULT_MEETING_CODE).strip().upper()
        raw_dir = st.text_input("Raw HTML folder", value=DEFAULT_RAW_DIR)
        db_path = st.text_input("SQLite DB", value=DEFAULT_DB)
        feature_csv = st.text_input("Feature CSV", value=DEFAULT_FEATURES)
        track_pars_path = st.text_input("Track pars JSON", value=DEFAULT_TRACK_PARS)
        odds_dir = st.text_input("Odds output folder", value=DEFAULT_ODDS_DIR)

        st.header("Actions")
        refresh_clicked = st.button("Refresh Meeting", use_container_width=True)
        rebuild_clicked = st.button("Build Features Only", use_container_width=True)
        score_clicked = st.button("Score Meeting", use_container_width=True)
        refresh_score_clicked = st.button("Refresh And Score", use_container_width=True)
        export_map_audit_clicked = st.button("Export Map Audit CSV", use_container_width=True)

    _show_status()

    if refresh_clicked or refresh_score_clicked:
        with st.spinner(f"Refreshing {meeting_code}..."):
            result = refresh_meeting(
                meeting_code=meeting_code,
                raw_dir=raw_dir,
                db_path=db_path,
                csv_path=feature_csv,
                track_pars_path=track_pars_path or None,
            )
        st.session_state["last_refresh_result"] = result
        st.session_state["last_scored_meeting"] = None
        st.success(f"Refreshed {meeting_code} and rebuilt features.")

    if rebuild_clicked:
        with st.spinner("Rebuilding features..."):
            feature_path = build_feature_dataset(
                db_path=db_path,
                csv_path=feature_csv,
                track_pars_path=track_pars_path or None,
            )
        st.success(f"Wrote feature dataset to {feature_path}")

    if score_clicked or refresh_score_clicked:
        with st.spinner(f"Scoring {meeting_code}..."):
            feature_rows = load_feature_rows(feature_csv)
            meeting_scores = score_meeting_rows(feature_rows, meeting_code)
            st.session_state["last_score_rows"] = feature_rows
            st.session_state["last_scored_meeting"] = {
                "meeting_code": meeting_code,
                "meeting_scores": meeting_scores,
            }
            odds_path = Path(odds_dir) / f"{meeting_code}_odds.csv"
            write_scored_rows_csv(flatten_meeting_scores(meeting_code, meeting_scores), odds_path)
            st.session_state["last_comparison"] = _load_comparison(db_path, meeting_code, meeting_scores)
            st.session_state["last_map_comparison"] = _load_map_comparison(db_path, meeting_code, feature_rows)
        st.success(f"Scored {meeting_code} and saved odds to {odds_path}")

    if export_map_audit_clicked:
        with st.spinner(f"Building map audit for {meeting_code}..."):
            feature_rows = load_feature_rows(feature_csv)
            map_comparison = _load_map_comparison(db_path, meeting_code, feature_rows)
            st.session_state["last_score_rows"] = feature_rows
            st.session_state["last_map_comparison"] = map_comparison
            map_audit_path = Path(odds_dir) / f"{meeting_code}_map_comparison.csv"
            if map_comparison and map_comparison.get("rows"):
                map_audit_path.parent.mkdir(parents=True, exist_ok=True)
                map_audit_path.write_bytes(_csv_bytes(map_comparison["rows"]))
                st.success(f"Saved map audit CSV to {map_audit_path}")
            else:
                st.warning(f"No map audit rows found for {meeting_code}.")

    _render_results_audit(meeting_code)
    _render_map_audit(meeting_code)

    scored_payload = st.session_state.get("last_scored_meeting")
    if not scored_payload:
        st.info("Use the sidebar to refresh, score, or export an audit for a meeting.")
        return

    meeting_scores = scored_payload["meeting_scores"]
    feature_rows = st.session_state.get("last_score_rows") or load_feature_rows(feature_csv)
    race_numbers = [race for race, rows in meeting_scores.items() if rows]
    if not race_numbers:
        st.warning(f"No scorable races found for {meeting_code}.")
        return

    col1, col2 = st.columns([1, 2])
    with col1:
        selected_race = st.selectbox("Race", race_numbers, index=0)
    with col2:
        odds_download_rows = flatten_meeting_scores(meeting_code, meeting_scores)
        st.download_button(
            "Download meeting odds CSV",
            data=_csv_bytes(odds_download_rows),
            file_name=f"{meeting_code}_odds.csv",
            mime="text/csv",
            use_container_width=True,
        )

    race_scored = meeting_scores.get(selected_race, [])
    st.subheader(f"{meeting_code} Race {selected_race} Odds")
    _render_scored_race_table(race_scored)

    race_feature_rows = _race_feature_rows(feature_rows, meeting_code, selected_race)
    if race_feature_rows:
        st.subheader(f"{meeting_code} Race {selected_race} Features")
        st.dataframe(race_feature_rows, use_container_width=True, hide_index=True)

        st.download_button(
            "Download race feature CSV",
            data=_csv_bytes(race_feature_rows),
            file_name=f"{meeting_code}_R{selected_race}_features.csv",
            mime="text/csv",
            use_container_width=True,
        )

        st.download_button(
            "Download race odds CSV",
            data=_csv_bytes([{"meeting_code": meeting_code, "race_number": selected_race, **row} for row in race_scored]),
            file_name=f"{meeting_code}_R{selected_race}_odds.csv",
            mime="text/csv",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
