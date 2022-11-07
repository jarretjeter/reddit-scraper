import datetime as dt
import glob
import logging
from logging import INFO
import os
import pandas as pd
import praw
from psaw import PushshiftAPI
import re
import sys
from time import sleep
import typer

logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

reddit_app = typer.Typer()

CLIENT_ID = os.environ.get("REDDSCRP_PU_SCRIPT")
SECRET_TOKEN = os.environ.get("REDDSCRP_SECRET")
headers = {"User-Agent": "reddscrape/0.0.1"}

reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=SECRET_TOKEN, user_agent=headers)



def get_date_range(df: pd.DataFrame) -> dt.date:
    """
    Parse a pd.Dataframe to get the earliest and latest date
    """
    df["Date"] = pd.to_datetime(df["Date"])
    min_date = min(df['Date'].dt.date)
    max_date = max(df['Date'].dt.date)
    return min_date, max_date


@reddit_app.command("merge_dfs")
def merge_dfs(subreddit: str, query: str, type: str) -> pd.DataFrame:
    """
    Merge a df from similar csv's and save to csv
    types "t" for targeting thread dfs, "c" for targeting comment dfs
    """
    types = ["t", "c"]
    if type not in types:
        raise ValueError("Invalid type parameter. Expected 't' for threads or 'c' for comments")
    type = "threads" if type == "t" else "comments"
    query = query.replace(" ", "_")
    sub_dir = f"./data/{subreddit}_{query}"
    if not os.path.exists(sub_dir): os.mkdir(sub_dir)
    
    csv_list = [file for file in glob.glob(f"{sub_dir}/{subreddit}_{query}_{type}*.csv")]
    logger.info("Merging dataframes")
    merged_df = pd.concat([pd.read_csv(csv) for csv in csv_list])
    merged_df.sort_values(by=["Date"], inplace=True)
    dates = get_date_range(merged_df)
    min_date = dates[0]
    max_date = dates[1]
    filename = f"/merged_{subreddit}_{query}_{type}_{min_date}-{max_date}.csv"
    logger.info("Saving new df to csv file.")
    merged_df.to_csv(f"{sub_dir}{filename}", index=False)
    return merged_df


@reddit_app.command("fetch_threads")
def fetch_threads(subreddit: str, query: str, limit=None) -> pd.DataFrame:
    """
    Fetch a subreddit's threads/posts about a particular subject
    """

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

    data_dict = {
        "ID": [], "Title" : [], "Subreddit": [], "Date": [], "Author": [], "Upvotes": [], "Ratio": [], "Num_Comments": [], "URL": []
        }
    batch = 0
    query = query.replace(" ", "_")
    sub_dir = f"./data/{subreddit}_{query}"
    if not os.path.exists(sub_dir): os.mkdir(sub_dir)

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
            sleep(1.0)
            logger.info("fetching next thread")
        else:
            batch += 1
            df = pd.DataFrame(data_dict)
            logger.info("Batch size 500 reached.")
            df["Author"].fillna("[deleted]", inplace=True)
            dates = get_date_range(df)
            min_date = dates[0]
            max_date = dates[1]
            filename = f"/{subreddit}_{query}_threads_{min_date}-{max_date}.csv"
            logger.info("Saving batch to csv file.")
            df.to_csv(f"{sub_dir}{filename}", index=False)
            # Reset data dictionary to save memory
            data_dict = {
                "ID": [], "Title" : [], "Subreddit": [], "Date": [], "Author": [], "Upvotes": [], "Ratio": [], "Num_Comments": [], "URL": []
                }

    df = pd.DataFrame(data_dict)
    logger.info("Fetching complete. Saving to csv file.")
    df["Author"].fillna("[deleted]", inplace=True)
    dates = get_date_range(df)
    min_date = dates[0]
    max_date = dates[1]
    filename = f"/{subreddit}_{query}_threads_{min_date}-{max_date}.csv"
    df.to_csv(f"{sub_dir}{filename}", index=False)
    # Merge
    if batch >= 1:
        merged_df = merge_dfs(subreddit, query, type="t")
        return merged_df
    else:
        return df


@reddit_app.command("fetch_comments")
def fetch_comments(subreddit, query, limit=None):
    """
    Fetch a subreddit's user comments about a particular subject
    """

    api = PushshiftAPI(reddit)
    limit = int(limit) if limit != None else limit

    comments = api.search_comments(
        subreddit=subreddit,
        q=query,
        after=int(dt.datetime(2018, 1, 1).timestamp()) - 1,
        before=int(dt.datetime.now().timestamp()),
        limit=limit
    )

    data_dict = {
        "ID": [], "Thread_Title": [], "Comment": [], "Date": [], "Author": [], "Upvotes": [], "Subreddit": [], "URL": []
        }
    batch = 0
    query = query.replace(" ", "_")
    sub_dir = f"./data/{subreddit}_{query}"
    if not os.path.exists(sub_dir): os.mkdir(sub_dir)
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
            batch += 1
            df = pd.DataFrame(data_dict)
            logger.info("Batch size 500 reached")
            df["Author"].fillna("[deleted]", inplace=True)
            dates = get_date_range(df)
            min_date = dates[0]
            max_date = dates[1]
            filename = f"/{subreddit}_{query}_comments_{min_date}-{max_date}.csv"
            logger.info("Saving batch to csv file.")
            df.to_csv(f"{sub_dir}{filename}", index=False)
            # Reset data dictionary to save memory
            data_dict = {
                "ID": [], "Thread_Title": [], "Comment": [], "Date": [], "Author": [], "Upvotes": [], "Subreddit": [], "URL": []
                }

    df = pd.DataFrame(data_dict)
    logger.info("Fetching complete. Saving to csv file.")
    df["Author"].fillna("[deleted]", inplace=True)
    dates = get_date_range(df)
    min_date = dates[0]
    max_date = dates[1]
    filename = f"/{subreddit}_{query}_comments_{min_date}-{max_date}.csv"
    df.to_csv(f"{sub_dir}{filename}", index=False)
    if batch >= 1:
        merged_df = merge_dfs(subreddit, query, type="c")
        return merged_df
    else:
        return df


if __name__ == "__main__":
    reddit_app()