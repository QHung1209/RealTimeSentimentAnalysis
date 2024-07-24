import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt


# Function to highlight NER labels in the content
def highlight_ner(content, ner_label):
    if ner_label != "Unknown":
        entities = ner_label.split(", ")
        for entity in entities:
            text, label = entity.split(": ")
            if label == "BOOK":
                content = content.replace(
                    text, f'<mark style="background-color: yellow">{text}</mark>'
                )
            elif label == "AUTHOR":
                content = content.replace(
                    text, f'<mark style="background-color: red">{text}</mark>'
                )
    return content

# Path to the result folder
result_folder_path = "./result"

# Function to find CSV files in the result folder
def find_csv_files(folder_path):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    return csv_files

# Function to load CSV file and apply NER highlighting
@st.cache_data
def load_and_highlight_csv(file_path):
    df = pd.read_csv(file_path, header=None, names=["content", "NER_label", "predict"])
    df["highlighted_content"] = df.apply(
        lambda row: highlight_ner(row["content"], row["NER_label"]), axis=1
    )
    df["predict"] = df["predict"].map({0.0: "Negative", 1.0: "Positive", 2.0: "Neutral"})
    return df

csv_files = find_csv_files(result_folder_path)

if csv_files:
    all_dfs = []
    for csv_file in csv_files:
        file_path = os.path.join(result_folder_path, csv_file)
        df = load_and_highlight_csv(file_path)
        all_dfs.append(df)

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Display the highlighted content and predictions in a table
    col1, col2 = st.columns([2, 1])
    
    with col1:
        for _, row in combined_df.iterrows():
            sub_col1, sub_col2 = st.columns(2)
            with sub_col1:
                st.markdown(row["highlighted_content"], unsafe_allow_html=True)
            with sub_col2:
                st.write(row["predict"])
    
    with col2:
        # Display prediction statistics as a pie chart
        st.write("### Prediction Statistics")
        prediction_counts = combined_df["predict"].value_counts()
        fig, ax = plt.subplots()
        ax.pie(prediction_counts, labels=prediction_counts.index, autopct='%1.1f%%')
        ax.axis("equal")  # Equal aspect ratio ensures that pie is drawn as a circle.
        st.pyplot(fig)

else:
    st.write(
        f"No CSV files found in {result_folder_path}. Please upload a CSV file manually."
    )
