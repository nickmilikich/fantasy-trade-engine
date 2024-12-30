import io
import streamlit as st


def download_dataframe(df, filename):
    csv = io.StringIO()
    df.to_csv(csv, index=False)
    st.download_button(
        label="Download Full Data as CSV",
        data=csv.getvalue(),
        file_name=filename,
        mime="text/csv",
    )
