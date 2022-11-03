import datetime as dt
import glob
import logging
from logging import INFO
import os
from time import sleep
import pandas as pd
import praw
from psaw import PushshiftAPI
import re
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


# WRITE CODE TO MERGE BATCH CSV'S
def date_range(df:pd.DataFrame) -> dt.date:
    """
    Parse a pd.Dataframe to get the earliest and latest date
    """
    df["Date"] = pd.to_datetime(df["Date"])
    min_date = min(df['Date'].dt.date)
    max_date = max(df['Date'].dt.date)
    return min_date, max_date


def merge_dfs(subreddit: str, query: str, date_list: list) -> pd.DataFrame:
    
    csv_list = [file for file in glob.glob(f"./data/{subreddit}_{query}*")]
    merged_df = pd.concat([pd.read_csv(c) for c in csv_list])
    return merged_df


@reddit_app.command("fetch_threads")
def fetch_threads(subreddit: str, query: str, limit=None, csv=None) -> pd.DataFrame:

    data_dict = {"ID": [], "Title" : [], "Subreddit": [], "Date": [], "Author": [], "Upvotes": [], "Ratio": [], "Num_Comments": [], "URL": []}
    date_list = []

    api = PushshiftAPI(reddit)
    limit = int(limit) if limit != None else limit
    threads = api.search_submissions(
        subreddit=subreddit,
        q=query,
        after=int(dt.datetime(2012, 1, 1).timestamp()) - 1,
        before=int(dt.datetime.now().timestamp()),
        limit=limit
    )
    meta_list = [thread.__dict__ for thread in threads]

    for meta_dict in meta_list:
        if len(data_dict["ID"]) < 500:
            data_dict["ID"] += [meta_dict["id"]]
            data_dict["Title"] += [meta_dict["title"]]
            data_dict["Subreddit"] += [meta_dict["subreddit_name_prefixed"]]
            data_dict["Date"] += [dt.datetime.fromtimestamp(meta_dict["created_utc"])]
            data_dict["Author"] += [meta_dict["author"]]
            data_dict["Upvotes"] += [meta_dict["ups"]]
            data_dict["Ratio"] += [meta_dict["upvote_ratio"]]
            data_dict["Num_Comments"] += [meta_dict["num_comments"]]
            data_dict["URL"] += [f"https://www.reddit.com{meta_dict['permalink']}"]
            logger.info("fetching next thread")
        else:
            df = pd.DataFrame(data_dict)
            if csv:
                logger.info("Batch size 500 reached. Saving current batch to csv file")
                query = query.replace(" ", "_")
                df["Author"].fillna("[deleted]", inplace=True)
                # df["Date"] = pd.to_datetime(df["Date"])
                # min_date = min(df['Date'].dt.date)
                # max_date = max(df['Date'].dt.date)
                dates = date_range(df)
                min_date = dates[0]
                max_date = dates[1]
                date_list.append(min_date)
                date_list.append(max_date)
                df.to_csv(f"./data/{subreddit}_{query}_threads-{min_date}-{max_date}.csv", index=False)
            # Reset data dictionary to save memory
            data_dict = {"ID": [], "Title" : [], "Subreddit": [], "Date": [], "Author": [], "Upvotes": [], "Ratio": [], "Num_Comments": [], "URL": []}
        
    df = pd.DataFrame(data_dict)
    if csv:
        logger.info("Saving final batch to csv file")
        query = query.replace(" ", "_")
        df["Author"].fillna("[deleted]", inplace=True)
        # df["Date"] = pd.to_datetime(df["Date"])
        # min_date = min(df['Date'].dt.date)
        # max_date = max(df['Date'].dt.date)
        dates = date_range(df)
        min_date = dates[0]
        max_date = dates[1]
        date_list.append(min_date)
        date_list.append(max_date)
        df.to_csv(f"./data/{subreddit}_{query}_threads-{min_date}-{max_date}.csv", index=False)
        # Merge
        merged_df = merge_dfs(subreddit, query)
        # Update min/max_date to reflect the date ranges in the list
        min_date = min(date_list)
        max_date = max(date_list)
        merged_df.to_csv(f"./data/merged_{subreddit}_{query}_threads_{min_date}-{max_date}.csv", index=False)

    return df


@reddit_app.command("fetch_comments")
def fetch_comments(subreddit, query, limit=None, csv=None):

    data_dict = {"ID": [], "Thread_Title": [], "Comment": [], "Date": [], "Author": [], "Upvotes": [], "Subreddit": [], "URL": []}
    api = PushshiftAPI(reddit)
    limit = int(limit) if limit != None else limit

    comments = list(api.search_comments(
        subreddit=subreddit,
        q=query,
        after=int(dt.datetime(2018, 1, 1).timestamp()) - 1,
        before=int(dt.datetime(2022, 10, 7).timestamp()),
        limit=limit
    ))

    past_url = ""

    for comment in comments:
        if len(data_dict["ID"]) < 500:
            data_dict["ID"] += [f"t1_{comment.id}"]
            current_submission = comment.submission
            data_dict["Thread_Title"] += [current_submission.title]
            url = f"https://reddit.com{comment.permalink}"
            submission_id = re.search("(comments)\/\w*\/", url).group()
            # Only need the selftext once per thread
            if submission_id != past_url and current_submission.selftext != "":
                current_submission.selftext = current_submission.selftext.replace("\n", "").replace("\r", "")
                data_dict["Comment"] += [current_submission.selftext]
                data_dict["Date"] += [dt.datetime.fromtimestamp(int(comment.created_utc))]
                data_dict["Author"] += [comment.author]
                data_dict["Upvotes"] += [comment.score]
                data_dict["Subreddit"] += [comment.subreddit]
                data_dict["URL"] += [url]
                sleep(1.0)
                logger.info("Fetching next comment")

            else:
                comment.body = comment.body.replace("\n", "").replace("\r", "")
                data_dict["Comment"] += [comment.body]
                data_dict["Date"] += [dt.datetime.fromtimestamp(int(comment.created_utc))]
                data_dict["Author"] += [comment.author]
                data_dict["Upvotes"] += [comment.score]
                data_dict["Subreddit"] += [comment.subreddit]
                data_dict["URL"] += [f"https://reddit.com{comment.permalink}"]
                sleep(1.0)
                logger.info("Fetching next comment")
        else:
            df = pd.DataFrame(data_dict)
            if csv:
                logger.info("Batch size 500 reached. Saving current batch to csv file")
                query = query.replace(" ", "_")
                df["Author"].fillna("[deleted]", inplace=True)
                df["Date"] = pd.to_datetime(df["Date"])
                min_date = min(df['Date'].dt.date)
                max_date = max(df['Date'].dt.date)
                df.to_csv(f"./data/{subreddit}_{query}_comments-{min_date}-{max_date}.csv", index=False)
            # Reset data dictionary to save memory
            data_dict = {"ID": [], "Thread_Title": [], "Comment": [], "Date": [], "Author": [], "Upvotes": [], "Subreddit": [], "URL": []}

    df = pd.DataFrame(data_dict)
    if csv:
        logger.info("Saving final batch to csv file")
        query = query.replace(" ", "_")
        df["Author"].fillna("[deleted]", inplace=True)
        df["Date"] = pd.to_datetime(df["Date"])
        min_date = min(df['Date'].dt.date)
        max_date = max(df['Date'].dt.date)
        df.to_csv(f"./data/{subreddit}_{query}_comments-{min_date}-{max_date}.csv", index=False)
    return df


if __name__ == "__main__":
    reddit_app()