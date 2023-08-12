
import streamlit as st
from Functions import ReconcileDocs

def intro():
    import streamlit as st

    st.write("# Welcome  👋")
    st.sidebar.success("Select an Action")

    st.markdown(
        """
        **Use Case:**
        - **File Upload:** Users can upload two files, File 463 and File 322, in CSV or Excel formats containing specific employee financial data.
        - **Reconciliation Process:** The application reconciles data based on the EMPLOYEE number, merging the data and calculating the final amount for each matching employee.
        - **Data Structuring:** The result includes details such as employee number, name, final amount, deduction codes, vendor names, and initial amounts from both files.
        - **Display and Download Options:** Users can view the number of records and the first 50 reconciliations, and download the complete reconciled data in CSV or Excel format.
        
        **👈 Select an action from the dropdown on the left.**
        """
    )


page_names_to_funcs = {
    "—": intro,
    "Reconcile 463 and 322 ":ReconcileDocs
  
    
}

demo_name = st.sidebar.selectbox("Choose a Action", page_names_to_funcs.keys())
page_names_to_funcs[demo_name]()