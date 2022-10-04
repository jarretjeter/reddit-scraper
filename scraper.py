from datetime import datetime
import logging
from logging import INFO
import os
import pandas as pd
import praw
import typer
import sys

logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

reddit_app = typer.Typer()

CLIENT_ID = os.environ.get("REDDSCRP_PU_SCRIPT")
SECRET_TOKEN = os.environ.get("REDDSCRP_SECRET")

headers = {"User-Agent": "reddscrape/0.0.1"}

reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=SECRET_TOKEN, user_agent=headers)


@reddit_app.command("threads")
def get_threads(reddit_group: str, subject: str, csv=None) -> pd.DataFrame:
    """
    Loops through the threads returned from a subreddit's search results to retrieve the titles, links, and other metadata.\n
    Outputs to a csv\n
    "reddit_group" is the subreddit to search through. ex: 'boxing'\n
    "subject" is the topic you want to search about. ex: 'ali'
    """
    submission_id_list = []
    thread_titles = []
    date_created = []
    authors = []
    thread_upvotes = []
    upvote_ratio = []
    total_comments = []
    url_list = []
    subreddit = reddit.subreddit(reddit_group)
    logger.info(f"Searching subreddit '{reddit_group}' for '{subject}'")
    for submission in subreddit.search(subject, limit=None):
        sub_id = submission.id
        title = submission.title
        date = datetime.fromtimestamp(submission.created_utc)
        author = submission.author
        score = submission.score
        ratio = submission.upvote_ratio
        num_comments = submission.num_comments
        url = "https://www.reddit.com" + submission.permalink
        submission_id_list.append(sub_id)
        thread_titles.append(title)
        date_created.append(date)
        authors.append(author)
        thread_upvotes.append(score)
        upvote_ratio.append(ratio)
        total_comments.append(num_comments)
        url_list.append(url)
    data_dict = {"ID": submission_id_list, "Title" : thread_titles, "Date_Created": date_created, "Author": authors, "Upvotes": thread_upvotes, "Upvote_Ratio": upvote_ratio, "Total_Comments": total_comments, "URL": url_list}
    logger.info(f"Creating dataframe for {subject} threads")
    df = pd.DataFrame(data=data_dict)
    if csv:
        logger.info("Saving to dataframe.")
        df.to_csv(f"./data/{subject}_threads.csv", index=False)
    return df


@reddit_app.command("comments")
def get_comments(reddit_group: str, subject: str, csv=None) -> pd.DataFrame:
    """
    Loop through reddit threads to retrieve the user comments concerning a subject\n
    Outputs to csv\n
    "reddit_group" is the subreddit to search through. ex: 'boxing'\n
    "subject" is the topic you want to search about. ex: 'ali'
    """
    titles_df = get_threads(reddit_group, subject)
    thread_titles = []
    comments = []
    authors = []
    upvotes = []
    i = 0
    logger.info(f"Retrieving comments from {subject} threads")
    while i < len(titles_df):
        row = titles_df.iloc[i]
        row_title = row["Title"]
        row_url = row["URL"]
        submission = reddit.submission(url=row_url)
        submission.comments.replace_more(limit=None)
        if submission.selftext != "":
            submission.selftext = submission.selftext.replace("\n", "")
            submission.selftext = submission.selftext.replace("\r", "")
            thread_titles.append(row_title)
            comments.append(submission.selftext)
            authors.append(submission.author)
            upvotes.append(submission.score)
        for comment in submission.comments.list():
            thread_titles.append(row_title)
            comment.body = comment.body.replace("\n", "")
            comment.body = comment.body.replace("\r", "")
            comments.append(comment.body)
            authors.append(comment.author)
            upvotes.append(comment.score)
        i += 1
    data_dict = {"Title": thread_titles, "Comment": comments, "Author": authors, "Upvotes": upvotes}
    logger.info("Creating dataframe for comments")
    df = pd.DataFrame(data=data_dict)
    if csv:
        logger.info("Saving to dataframe.")
        df.to_csv(f"./data/{subject}_comments.csv", index=False)
    return df


if __name__ == "__main__":
    reddit_app()