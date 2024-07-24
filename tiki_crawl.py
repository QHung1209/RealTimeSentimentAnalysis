import requests
import pandas as pd
import time
import re
import os

def fetch_reviews(product_id):
    api_url = "https://tiki.vn/api/v2/reviews"
    params = {
        "include": "comments",
        "product_id": product_id
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(api_url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        reviews = data.get('data', [])
        review_contents = [review['content'] for review in reviews if review['content']]
        return review_contents
    else:
        print(f"Failed to retrieve data for product_id {product_id}: {response.status_code}")
        return []

def preprocess_text(text):
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = text.lower()
    return text

def save_reviews_to_csv(product_id, reviews, folder='./data'):
    os.makedirs(folder, exist_ok=True)  # Create the folder if it doesn't exist
    df = pd.DataFrame(reviews, columns=['content'])
    df['content'] = df['content'].apply(preprocess_text)
    file_path = os.path.join(folder, f"{product_id}.csv")
    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"Saved reviews for product_id {product_id} to {file_path}")

def fetch_and_save_reviews(product_ids, delay=1, folder='./data'):
    for product_id in product_ids:
        reviews = fetch_reviews(product_id)
        if reviews:
            save_reviews_to_csv(product_id, reviews, folder)
        time.sleep(delay)  # To avoid hitting the API rate limit

# List of product_ids to fetch reviews for
product_ids = [480040, 199692508, 109017985]  # Add your product_ids here

fetch_and_save_reviews(product_ids, delay=1, folder='./data')
