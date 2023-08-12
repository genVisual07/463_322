import streamlit as st
import pandas as pd
import dask.dataframe as dd
from datetime import datetime
import os
from io import BytesIO

def reconcile_files(file_463, file_322):
    with st.spinner('Reading files...'):
        if file_463.name.endswith('.csv'):
            df_463 = dd.read_csv(file_463)
        else:
            df_463 = dd.from_pandas(pd.read_excel(file_463), npartitions=5)

        if file_322.name.endswith('.csv'):
            df_322 = dd.read_csv(file_322)
        else:
            df_322 = dd.from_pandas(pd.read_excel(file_322), npartitions=5)

        df_463['AMOUNT'] = df_463['AMOUNT'].replace(',', '', regex=True).astype(float)
        df_322['AMOUNT'] = df_322['AMOUNT'].replace(',', '', regex=True).astype(float)

    with st.spinner('Merging and reconciling files...'):
        merged_df = dd.merge(df_463, df_322, on='EMPLOYEE', suffixes=('_463', '_322'))
        merged_df['FINAL AMOUNT'] = merged_df['AMOUNT_463'] + merged_df['AMOUNT_322']

        result_df = merged_df[['EMPLOYEE', 'NAME_463', 'FINAL AMOUNT', 'DEDUCTION_463', 'VENDOR NAME_463', 'AMOUNT_463', 'DEDUCTION_322', 'VENDOR NAME_322', 'AMOUNT_322']]
        result_df = result_df.rename(columns={
            'NAME_463': 'NAME',
            'DEDUCTION_463': 'DEDUCTION CODE 463',
            'VENDOR NAME_463': 'VENDOR NAME (463)',
            'AMOUNT_463': 'Amount (463)',
            'DEDUCTION_322': 'DEDUCTION CODE 322',
            'VENDOR NAME_322': 'VENDOR NAME (322)',
            'AMOUNT_322': 'Amount (322)'
        })

        result = result_df.compute()

    with st.spinner('Identifying unmatched records...'):

        # Performing an outer join
        all_records = dd.merge(df_463, df_322, on='EMPLOYEE', how='outer', indicator=True)

        # Finding unmatched records from file 463
        unmatched_463 = all_records[all_records['_merge'] == 'left_only']
        unmatched_463 = unmatched_463[['EMPLOYEE', 'NAME_x', 'AMOUNT_x', 'VOTE_x', 'MINISTRY_x', 'DEDUCTION_x', 'VENDOR NAME_x']]
        unmatched_463 = unmatched_463.rename(columns={'NAME_x': 'NAME', 'AMOUNT_x': 'AMOUNT', 'VOTE_x': 'VOTE', 'MINISTRY_x': 'MINISTRY', 'DEDUCTION_x': 'DEDUCTION', 'VENDOR NAME_x': 'VENDOR NAME'})

        # Finding unmatched records from file 322
        unmatched_322 = all_records[all_records['_merge'] == 'right_only']
        unmatched_322 = unmatched_322[['EMPLOYEE', 'NAME_y', 'AMOUNT_y', 'VOTE_y', 'MINISTRY_y', 'DEDUCTION_y', 'VENDOR NAME_y']]
        unmatched_322 = unmatched_322.rename(columns={'NAME_y': 'NAME', 'AMOUNT_y': 'AMOUNT', 'VOTE_y': 'VOTE', 'MINISTRY_y': 'MINISTRY', 'DEDUCTION_y': 'DEDUCTION', 'VENDOR NAME_y': 'VENDOR NAME'})

        # Combining unmatched records from both files
        unmatched_records = pd.concat([unmatched_463.compute(), unmatched_322.compute()])

    return result, unmatched_records

def save_files(matched_df, unmatched_df):
    folder_name = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(folder_name, exist_ok=True)

    matched_filename = os.path.join(folder_name, f"matched_{datetime.now().strftime('%H-%M-%S')}.csv")
    matched_df.to_csv(matched_filename, index=False)

    unmatched_filename = os.path.join(folder_name, f"unmatched_{datetime.now().strftime('%H-%M-%S')}.csv")
    unmatched_df.to_csv(unmatched_filename, index=False)

def ReconcileDocs():
    st.title('Reconcile Employee Files')

    file_463 = st.file_uploader("Upload File 463 (CSV or Excel)", type=['csv', 'xlsx'])
    file_322 = st.file_uploader("Upload File 322 (CSV or Excel)", type=['csv', 'xlsx'])

    if file_463 and file_322:
        matched_records, unmatched_records = reconcile_files(file_463, file_322)

        st.write(f"Number of records in File 463: {len(dd.read_csv(file_463)) if file_463.name.endswith('.csv') else len(pd.read_excel(file_463))}")
        st.write(f"Number of records in File 322: {len(dd.read_csv(file_322)) if file_322.name.endswith('.csv') else len(pd.read_excel(file_322))}")
        st.write("Reconciliation complete! Here are the first 50 reconciliations:")
        st.dataframe(matched_records.head(50))

        if not unmatched_records.empty:
            st.write("Unmatched records found! Here are the first 10:")
            st.dataframe(unmatched_records.head(10))
            st.download_button(label="Download Unmatched Records", data=unmatched_records.to_csv(index=False), file_name="unmatched_records.csv", mime="text/csv")

        excel_file = BytesIO()
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            matched_records.to_excel(writer, index=False)
        excel_file.seek(0)
        st.download_button(label="Download Reconciled Data as Excel", data=excel_file, file_name="reconciled_data.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        with st.spinner('Saving files...'):
            save_files(matched_records, unmatched_records)

       

    else:
        st.write("Please upload both files to proceed.")

if __name__ == "__main__":
    ReconcileDocs()
