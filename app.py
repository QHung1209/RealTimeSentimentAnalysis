import streamlit as st
import pandas as pd
import os
import time

# Function to highlight NER labels in the content
def highlight_ner(content, ner_label):
    if ner_label != "Không có":
        entities = ner_label.split(", ")
        for entity in entities:
            text, label = entity.split(": ")
            if label == "BOOK":
                content = content.replace(text, f'<mark style="background-color: yellow">{text}</mark>')
            elif label == "AUTHOR":
                content = content.replace(text, f'<mark style="background-color: red">{text}</mark>')
    return content
    
# Path to the result folder
result_folder_path = "./result"

# Function to find CSV files in the result folder
def find_csv_files(folder_path):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    return csv_files

# Function to load CSV file and apply NER highlighting
@st.cache(allow_output_mutation=True)
def load_and_highlight_csv(file_path):
    df = pd.read_csv(file_path, header=None, names=['content', 'NER_label', 'predict'])
    df['highlighted_content'] = df.apply(lambda row: highlight_ner(row['content'], row['NER_label']), axis=1)
    return df

# Main Streamlit app code
def main():
    csv_files = find_csv_files(result_folder_path)
    
    if csv_files:
        selected_file = st.selectbox("Select a CSV file", csv_files)
        file_path = os.path.join(result_folder_path, selected_file)
        
        # Load and highlight CSV file
        df = load_and_highlight_csv(file_path)
        
        # Display the highlighted content
        st.write("### Highlighted Content with NER Labels")
        for _, row in df.iterrows():
            st.markdown(row['highlighted_content'], unsafe_allow_html=True)
        
        # Optionally, visualize prediction statistics
        st.write("### Prediction Statistics")
        prediction_counts = df['predict'].value_counts()
        st.bar_chart(prediction_counts)
    
    else:
        st.write(f"No CSV files found in {result_folder_path}. Please upload a CSV file manually.")

# Run the Streamlit app
if __name__ == "__main__":
    while True:
        main()
        time.sleep(5)  # Check for updates every 5 seconds
