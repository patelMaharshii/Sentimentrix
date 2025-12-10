# Analysis

This folder contains Jupyter notebooks and scripts for exploring and analyzing the dataset used in this project. Each file demonstrates a different aspect of the data or a specific analysis workflow.

## Getting Started

To set up your environment and install all required Python packages, run:
```
pip install -r requirements.txt
```

## Dataset

### _posts datasets
This datasets gets all the main posts made by a given user in a subreddit

| Column                         | Description                                      |
|--------------------------------|--------------------------------------------------|
| **subreddit**                  | Name of the current subreddit                    |
| **post_id**                    | Posts ID                                         |
| **post_title**                 | Title of post                                    |
| **post_score**                 | Posts net score (upvotes - downvotes)            |
| **post_url**                   | Direct reddit posts URL                          |
| **post_content_url**           | If image exists, links directly to post image    |
| **post_text**                  | Text contents of the post                        |
| **timestamp**                  | UTC timestamp                                    |
| **post_upvote_ratio**          | Upvote to downvote ratio                         |
| **post_ups**                   | Current post upvotes                             |
| **post_total_awards_received** | Post awards received                             |
| **post_link_flair_text**       | Post flairs (categories)                         |
| **post_author**                | Post author username                             |
| **post_num_comments**          | Post number of comments                          |
| **has_images**                 | Boolean if post have image or not                |
| **num_images**                 | Number of images the post contains (gallery)     |
| **is_gallery**                 | Boolean if gallery is exist                      |
| **content_type**               | Describes which format the content is            |


### _images datasets
This dataset gets the images made by any user in a subreddit (comments and posts)

| Column                         | Description                                             |
|--------------------------------|---------------------------------------------------------|
| **subreddit**                  | Name of the current subreddit                           |
| **post_id**                    | Posts ID                                                |
| **comment_id**                 | Comment_ID                                              |
| **image_index**                | Current index of the image (gallery style)              |
| **image_url**                  | Direct image URL                                        |
| **image_source**               | Indicates where image came from (comment, gallery, post |
| **image_type**                 | Indicates if its embedded, gallery or direct            |
| **media_id**                   | Media ID                                                |


### _comments datasets
This datasets gets all the comments from a parent post and their replies below it

| Column                         | Description                                      |
|--------------------------------|--------------------------------------------------|
| **post_id**                    | Posts ID                                         |
| **comment_id**                 ||
| **comment_text**               ||
| **comment_score**              ||
| **comment_author**             ||
| **comment_created_utc**        ||
| **parent_id**                  ||
| **reply_to_id**                ||
| **comment_sentiment**          ||
| **has_images**                 ||
| **num_images**                 ||
| **image_urls**                 ||
| **subreddit**                  ||
