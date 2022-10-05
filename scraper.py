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

@reddit_app.command("sub_data")
def submission_data(submission: str, output=None) -> list:
    """
    Retrieve data on a single reddit submission(thread)
    Requires a reddit submission URL or ID
    """
    reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=SECRET_TOKEN, user_agent=headers)
    submission = reddit.submission(url=submission) if "https:" in str(submission) else reddit.submission(id=submission)

    sub_id = submission.id
    sub_title = submission.title
    date = datetime.fromtimestamp(submission.created_utc)
    author = submission.author
    upvotes = submission.score
    upvote_ratio = submission.upvote_ratio
    num_comments = submission.num_comments
    url =  f"https://www.reddit.com{submission.permalink}"
    if output:
        print(f"ID:{sub_id}, Title:{sub_title}, Date:{date}, Author:{author}, Upvotes:{upvotes}, Upvote Ratio:{upvote_ratio}, # of Comments:{num_comments}, URL:{url}")
    return sub_id, sub_title, date, author, upvotes, upvote_ratio, num_comments, url


@reddit_app.command("all_subs")
def get_threads(reddit_group: str, subject: str, csv=None) -> pd.DataFrame:
    """
    Loops through the threads returned from a subreddit's search sub_datas to retrieve the titles, links, and other metadata.\n
    Outputs to a csv\n
    "reddit_group" is the subreddit to search through. ex: 'boxing'\n
    "subject" is the topic you want to search about. ex: 'ali'
    """

    data_dict = {"ID": [], "Title" : [], "Date_Created": [], "Author": [], "Upvotes": [], "Upvote_Ratio": [], "Total_Comments": [], "URL": []}

    reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=SECRET_TOKEN, user_agent=headers)
    subreddit = reddit.subreddit(reddit_group)

    logger.info(f"Searching subreddit '{reddit_group}' for '{subject}'")
    for submission in subreddit.search(subject, limit=None):
        submission_id = submission.id
        submission_title = submission.title
        date = datetime.fromtimestamp(submission.created_utc)
        author = submission.author
        upvotes = submission.score
        upvote_ratio = submission.upvote_ratio
        num_comments = submission.num_comments
        url = "https://www.reddit.com" + submission.permalink

        data_dict["ID"] += [submission_id]
        data_dict["Title"] += [submission_title]
        data_dict["Date"] += [date]
        data_dict["Author"] += [author]
        data_dict["Upvotes"] += [upvotes]
        data_dict["Upvote_Ratio"] += [upvote_ratio]
        data_dict["Total_Comments"] += [num_comments]
        data_dict["URL"] += [url]
    logger.info(f"Creating dataframe for {subject} threads")
    df = pd.DataFrame(data=data_dict)
    if csv:
        logger.info("Saving to dataframe.")
        df.to_csv(f"./data/{subject}_threads.csv", index=False)
    return df


@reddit_app.command("comments")
def comment_data(submission, csv=None) -> list:
    """
    Retrieve all of the reddit comments for a single submission(thread)
    Requires a reddit submission URL or ID
    """
    data_dict = {"Title": [], "Comment": [], "Author": [], "Upvotes": [], "Downvotes": []}

    reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=SECRET_TOKEN, user_agent=headers)
    submission = reddit.submission(url=submission) if "https:" in str(submission) else reddit.submission(id=submission)
    submission.comments.replace_more(limit=None)

    if submission.selftext != "":
        submission.selftext = submission.selftext.replace("\n", "")
        submission.selftext = submission.selftext.replace("\r", "")
        data_dict["Title"] += [submission.title]
        data_dict["Comment"] += [submission.selftext]
        data_dict["Author"] += [submission.author]
        data_dict["Upvotes"] += [submission.score]
        data_dict["Downvotes"] += [submission.downs]

    for comment in submission.comments.list():
        comment.body = comment.body.replace("\n", "")
        comment.body = comment.body.replace("\r", "")
        data_dict["Title"] += [submission.title]
        data_dict["Comment"] += [comment.body]
        data_dict["Author"] += [comment.author]
        data_dict["Upvotes"] += [comment.score]
        data_dict["Downvotes"] += [comment.downs]

    if csv:
        logger.info("Saving to csv file")
        df = pd.DataFrame(data=data_dict)
        df.to_csv(f"./data/{submission.title}.csv", index=False)
    return data_dict["Title"], data_dict["Comment"], data_dict["Author"], data_dict["Upvotes"], data_dict["Downvotes"]


@reddit_app.command("all_comments")
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
        reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=SECRET_TOKEN, user_agent=headers)
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