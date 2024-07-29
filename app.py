import streamlit as st
import pandas as pd
import chardet
import re
from unidecode import unidecode
import logging

# Set up logging
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

def read_csv(file):
    try:
        raw_data = file.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        logging.info(f"Detected encoding for {file.name}: {encoding}")
        file.seek(0)
        return pd.read_csv(file, encoding=encoding)
    except Exception as e:
        logging.error(f"Error reading CSV file {file.name}: {e}")
        raise

def preprocessing(df):
    try:
        columns_to_keep = ['title', 'year', 'journal', 'authors', 'doi']
        df = df[columns_to_keep]
        df['title'] = df['title'].apply(lambda x: unidecode(x))  # Convert Unicode characters to closest ASCII equivalent
        df['title'] = df['title'].apply(lambda x: re.sub(r'[^\w\s]', ' ', x))  # Remove non-word characters
        df['title_normalized'] = df['title'].str.lower()
        logging.info(f"Preprocessing completed for DataFrame")
        return df
    except Exception as e:
        logging.error(f"Error preprocessing DataFrame: {e}")
        raise

def find_and_split_duplicates(df1, df2, df3=None):
    try:
        df1 = preprocessing(df1)
        df2 = preprocessing(df2)
        combined_df = pd.concat([df1, df2, df3]) if df3 is not None else pd.concat([df1, df2])
        logging.info(f"Combined DataFrame shape: {combined_df.shape}")

        with_doi = combined_df.dropna(subset=['doi'])
        without_doi = combined_df[combined_df['doi'].isna()]

        duplicates_with_doi = with_doi[with_doi.duplicated(subset='doi', keep=False)]
        duplicates_without_doi = without_doi[without_doi.duplicated(subset=['title_normalized', 'year'], keep=False)]

        duplicates_df = pd.concat([duplicates_with_doi, duplicates_without_doi]).drop_duplicates()
        logging.info(f"Duplicates DataFrame shape: {duplicates_df.shape}")

        shared_in_A = df1[df1['doi'].isin(duplicates_df['doi']) |
                          df1[['title_normalized', 'year']].apply(tuple, axis=1).isin(
                              duplicates_df[['title_normalized', 'year']].apply(tuple, axis=1))]
        shared_in_B = df2[df2['doi'].isin(duplicates_df['doi']) |
                          df2[['title_normalized', 'year']].apply(tuple, axis=1).isin(
                              duplicates_df[['title_normalized', 'year']].apply(tuple, axis=1))]
        unique_in_A = df1[~df1.index.isin(shared_in_A.index)]
        unique_in_B = df2[~df2.index.isin(shared_in_B.index)]

        if df3 is not None:
            shared_in_C = df3[df3['doi'].isin(duplicates_df['doi']) |
                              df3[['title_normalized', 'year']].apply(tuple, axis=1).isin(
                                  duplicates_df[['title_normalized', 'year']].apply(tuple, axis=1))]
            unique_in_C = df3[~df3.index.isin(shared_in_C.index)]
            return unique_in_A.drop(columns=['title_normalized']), unique_in_B.drop(columns=['title_normalized']), unique_in_C.drop(columns=['title_normalized']), duplicates_df.drop(columns=['title_normalized'])
        else:
            return unique_in_A.drop(columns=['title_normalized']), unique_in_B.drop(columns=['title_normalized']), duplicates_df.drop(columns=['title_normalized'])
    except Exception as e:
        logging.error(f"Error finding and splitting duplicates: {e}")
        raise

def remove_duplicates_and_keep_one(df):
    try:
        df = preprocessing(df)
        unique_df = df.drop_duplicates(subset=['doi', 'title_normalized', 'year']).drop(columns=['title_normalized'])
        return unique_df
    except Exception as e:
        logging.error(f"Error removing duplicates: {e}")
        raise

def find_duplicates_in_one_file(df):
    try:
        df = preprocessing(df)
        with_doi = df.dropna(subset=['doi'])
        without_doi = df[df['doi'].isna()]
        duplicates_with_doi = with_doi[with_doi.duplicated(subset='doi', keep=False)]
        duplicates_without_doi = without_doi[without_doi.duplicated(subset=['title_normalized', 'year'], keep=False)]
        duplicates_df = pd.concat([duplicates_with_doi, duplicates_without_doi]).drop_duplicates()
        return duplicates_df.drop(columns=['title_normalized'])
    except Exception as e:
        logging.error(f"Error finding duplicates in one file: {e}")
        raise

# Streamlit UI
st.title("CSV Processor")

option = st.selectbox(
    'Choose processing option:',
    ('Remove duplicates from one file', 'Find and split duplicates between two files', 'Find and split duplicates between three files')
)

if option == 'Remove duplicates from one file':
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file is not None:
        df = read_csv(uploaded_file)
        unique_df = remove_duplicates_and_keep_one(df)
        st.write("Unique Entries:")
        st.dataframe(unique_df)
        csv = unique_df.to_csv(index=False).encode('utf-8')
        st.download_button("Download Unique Entries CSV", data=csv, file_name="unique_entries.csv", mime='text/csv')

elif option == 'Find and split duplicates between two files':
    uploaded_file_a = st.file_uploader("Choose first CSV file", type="csv")
    uploaded_file_b = st.file_uploader("Choose second CSV file", type="csv")
    if uploaded_file_a is not None and uploaded_file_b is not None:
        df_a = read_csv(uploaded_file_a)
        df_b = read_csv(uploaded_file_b)
        unique_in_a, unique_in_b, duplicates = find_and_split_duplicates(df_a, df_b)
        st.write("Unique in A:")
        st.dataframe(unique_in_a)
        st.write("Unique in B:")
        st.dataframe(unique_in_b)
        st.write("Duplicates:")
        st.dataframe(duplicates)
        csv_a = unique_in_a.to_csv(index=False).encode('utf-8')
        st.download_button("Download Unique in A CSV", data=csv_a, file_name="unique_in_a.csv", mime='text/csv')
        csv_b = unique_in_b.to_csv(index=False).encode('utf-8')
        st.download_button("Download Unique in B CSV", data=csv_b, file_name="unique_in_b.csv", mime='text/csv')
        csv_duplicates = duplicates.to_csv(index=False).encode('utf-8')
        st.download_button("Download Duplicates CSV", data=csv_duplicates, file_name="duplicates.csv", mime='text/csv')

elif option == 'Find and split duplicates between three files':
    uploaded_file_a = st.file_uploader("Choose first CSV file", type="csv")
    uploaded_file_b = st.file_uploader("Choose second CSV file", type="csv")
    uploaded_file_c = st.file_uploader("Choose third CSV file", type="csv")
    if uploaded_file_a is not None and uploaded_file_b is not None and uploaded_file_c is not None:
        df_a = read_csv(uploaded_file_a)
        df_b = read_csv(uploaded_file_b)
        df_c = read_csv(uploaded_file_c)
        unique_in_a, unique_in_b, unique_in_c, duplicates = find_and_split_duplicates(df_a, df_b, df_c)
        st.write("Unique in A:")
        st.dataframe(unique_in_a)
        st.write("Unique in B:")
        st.dataframe(unique_in_b)
        st.write("Unique in C:")
        st.dataframe(unique_in_c)
        st.write("Duplicates:")
        st.dataframe(duplicates)
        csv_a = unique_in_a.to_csv(index=False).encode('utf-8')
        st.download_button("Download Unique in A CSV", data=csv_a, file_name="unique_in_a.csv", mime='text/csv')
        csv_b = unique_in_b.to_csv(index=False).encode('utf-8')
        st.download_button("Download Unique in B CSV", data=csv_b, file_name="unique_in_b.csv", mime='text/csv')
        csv_c = unique_in_c.to_csv(index=False).encode('utf-8')
        st.download_button("Download Unique in C CSV", data=csv_c, file_name="unique_in_c.csv", mime='text/csv')
        csv_duplicates = duplicates.to_csv(index=False).encode('utf-8')
        st.download_button("Download Duplicates CSV", data=csv_duplicates, file_name="duplicates.csv", mime='text/csv')
