from datetime import datetime
import logging
from logging import INFO
import os
import pandas as pd
import praw
from psaw import PushshiftAPI
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


@reddit_app.command("fetch_threads")
def fetch_threads(subreddit: str, query: str, limit=None, csv=None) -> pd.DataFrame:

    data_dict = {"ID": [], "Title" : [], "Subreddit": [], "Date": [], "Author": [], "Upvotes": [], "Ratio": [], "Num_Comments": [], "URL": []}

    api = PushshiftAPI(reddit)
    threads = api.search_submissions(
        subreddit=subreddit,
        q=query,
        after=int(datetime(2012, 1, 1).timestamp()) - 1,
        before=int(datetime.now().timestamp()),
        limit=int(limit)
    )
    meta_list = [thread.__dict__ for thread in threads]

    for meta_dict in meta_list:
        data_dict["ID"] += [meta_dict["id"]]
        data_dict["Title"] += [meta_dict["title"]]
        data_dict["Subreddit"] += [meta_dict["subreddit_name_prefixed"]]
        data_dict["Date"] += [datetime.fromtimestamp(meta_dict["created_utc"])]
        data_dict["Author"] += [meta_dict["author"]]
        data_dict["Upvotes"] += [meta_dict["ups"]]
        data_dict["Ratio"] += [meta_dict["upvote_ratio"]]
        data_dict["Num_Comments"] += [meta_dict["num_comments"]]
        data_dict["URL"] += [f"https://www.reddit.com{meta_dict['permalink']}"]

        
    df = pd.DataFrame(data_dict)
    if csv:
        logger.info("Saving to csv file")
        query = query.replace(" ", "_")
        df.to_csv(f"./data/{query}_threads.csv", index=False)
    return df


@reddit_app.command("fetch_comments")
def fetch_comments(subreddit, query, limit=None, csv=None):

    data_dict = {"ID": [], "Thread_Title": [], "Comment": [], "Date": [], "Author": [], "Upvotes": [], "Downvotes": [], "Subreddit": [], "URL": []}

    api = PushshiftAPI(reddit)
    comments = list(api.search_comments(
        subreddit=subreddit,
        q=query,
        after=int(datetime(2012, 1, 1).timestamp()) - 1,
        before=int(datetime.now().timestamp()),
        limit=int(limit)
    ))

    for comment in comments:
        data_dict["ID"] += [f"t1_{comment.id}"]
        thread_id = reddit.info(fullnames=[comment.link_id])
        for item in thread_id:
            data_dict["Thread_Title"] += [item.title]
        comment.body = comment.body.replace("\n", "")
        comment.body = comment.body.replace("\r", "")
        data_dict["Comment"] += [comment.body]
        data_dict["Date"] += [datetime.fromtimestamp(int(comment.created_utc))]
        data_dict["Author"] += [comment.author]
        data_dict["Upvotes"] += [comment.score]
        data_dict["Downvotes"] += [comment.downs]
        data_dict["Subreddit"] += [comment.subreddit]
        data_dict["URL"] += [f"https://reddit.com{comment.permalink}"]

    df = pd.DataFrame(data_dict)
    if csv:
        logger.info("Saving to csv file")
        query = query.replace(" ", "_")
        df.to_csv(f"./data/{query}_threads.csv", index=False)
    return df


if __name__ == "__main__":
    reddit_app()