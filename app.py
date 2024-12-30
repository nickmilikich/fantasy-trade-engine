# app.py
import streamlit as st

from config import TEAM_COMPOSITION
from utils.io import download_dataframe
from utils.mapping import Mapping
from utils.services import get_recommended_trades


def main():
    st.title("Trade Recommendation App")

    # User inputs
    league_id = st.text_input("League ID", placeholder="Enter the league ID")
    year = st.number_input("Year", min_value=2000, max_value=2100, step=1, value=2024)

    mapping = Mapping(league_id=league_id)
    display_name = st.selectbox(
        "Display name",
        placeholder="Select your display name",
        options=mapping.league_id_to_display_names[league_id],
    )
    max_group_size = st.number_input("Max Group Size", min_value=1, max_value=4, step=1, value=1)
    positions = st.multiselect(
        "Positions",
        placeholder="Select positions to consider for trades (or leave blank to consider all)",
        options=TEAM_COMPOSITION["team_composition"].keys(),
    )
    other_users = st.multiselect(
        "Other Users",
        placeholder="Select users to consider for trades (or leave blank to consider all)",
        options=[
            user for user in mapping.league_id_to_display_names[league_id] if user != display_name
        ],
    )

    if positions == []:
        positions = None
    if other_users == []:
        other_users = None
    if other_users is not None:
        other_users = [mapping.display_name_to_user_id.get(user) for user in other_users]

    if st.button("Get Recommended Trades"):
        with st.spinner("Fetching recommended trades..."):
            trades_df = get_recommended_trades(
                league_id=league_id,
                year=year,
                user_id=mapping.display_name_to_user_id.get(display_name),
                max_group_size=max_group_size,
                positions=positions,
                other_users=other_users,
            )

            if trades_df.empty:
                st.warning("No recommended trades found.")
            else:
                st.success("Recommended trades fetched successfully!")

                st.subheader("Preview of Recommended Trades")
                st.dataframe(trades_df.head())

                # Download button
                download_dataframe(trades_df, "recommended_trades.csv")


if __name__ == "__main__":
    main()
