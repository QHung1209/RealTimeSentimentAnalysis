import requests
import pandas as pd
import time
import re

def fetch_reviews():
    # Define the API endpoint and parameters
    api_url = "https://tiki.vn/api/v2/reviews"
    params = {
        "limit": 10,
        "include": "comments",
        "product_id": 109017987
    }

    # Define headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # Send a GET request to the API with headers
    response = requests.get(api_url, params=params, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        # Extract the content of each review
        reviews = data.get('data', [])
        review_contents = [review['content'] for review in reviews if review['content']]
        return review_contents
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        return []

def preprocess_text(text):
    # Remove punctuation, special characters, and extra whitespace
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra whitespace
    # Convert to lowercase
    text = text.lower()
    return text

def save_reviews_to_csv(new_reviews, file_name='./data/data.csv'):
    # Load existing reviews from the CSV file
    try:
        df_existing = pd.read_csv(file_name, encoding='utf-8-sig')
        existing_reviews = df_existing['content'].tolist()
    except FileNotFoundError:
        df_existing = pd.DataFrame(columns=['content'])
        existing_reviews = []

    # Preprocess new reviews and remove null/empty entries
    new_reviews = [preprocess_text(review) for review in new_reviews if review.strip()]

    # Find new reviews that are not in the existing reviews
    new_unique_reviews = [review for review in new_reviews if review not in existing_reviews]

    if new_unique_reviews:
        # Create a DataFrame from the new unique reviews
        df_new = pd.DataFrame(new_unique_reviews, columns=['content'])

        # Append new reviews to the existing DataFrame
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

        # Save the combined DataFrame to the CSV file
        df_combined.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"Appended {len(new_unique_reviews)} new reviews to {file_name}")
    else:
        print("No new reviews to append")

# Main loop to check for new reviews every 5 seconds
while True:
    new_reviews = fetch_reviews()
    save_reviews_to_csv(new_reviews)
    time.sleep(5)
