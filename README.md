# Goodreads Books Data Analysis

## Project Overview
This project analyzes book data collected from a Google site containing data from the Goodreads API. The analysis is performed using Apache Spark with Python in Google Colab. The dataset includes information on books, reviews, and user interactions.

## Features & Analysis
- **Data Analysis using Spark**:
  - Most popular book genres
  - Most common languages for each genre
  - Most books written by an author
  - Most reviewed books
  - Most read books
  - Most "want to read" books
  - Most published books by year
  - Highest-rated book per genre
  - Most common double-genre combinations
  - Most frequent author-genre pairings

- **Machine Learning (Recommendation System)**:
  - Collaborative filtering using ALS (Alternating Least Squares)
  - Generate top 10 book recommendations per user
  - Generate top 10 user recommendations per book
  
## Dataset
The dataset consists of three main sections:
- **Books Metadata**: Includes book details, authors, book series, genres, etc.
- **Reviews**: Complete book reviews and spoiler reviews.
- **User Interactions**: Information on books shelved by users (read, want to read, etc.).

## Usage
- Run the Spark functions for various book analyses.
- Visualize the results using Tableau.
- Train and evaluate the book recommendation model using ALS.

## Results
- All analysis outputs are stored in CSV format.
- Visualizations are created using Tableau for better insights.
- The recommendation model provides personalized book suggestions.
