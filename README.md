# reddit-scraper

#### By Jarret Jeter

#### Reddit web-scraper project using the Python Reddit API Wrapper (praw) for the general purpose of getting information on user comments on a particular subject.

## Technologies Used

* _Azure Storage_
* _Python_
* _Pandas_
* _Praw_
* _Typer_

## Description

_With Python's praw package I instantiated a Reddit object using a Reddit application. The Reddit object can access many things through Reddit's API such as subreddits, users, topics, and comments. I currently have defined two functions in the scraper.py file, get_threads() and get_comments(). get_threads() takes the arguments of a subreddit to search through and a particular subject as the criteria (with an optional argument to save results to a csv file), returning a Pandas dataframe with data on thread id, thread title, date created, total comments, etc. get_comments also takes the same two arguments with an optional third, retrieving all of the comments made from the returned threads. I then have some functions defined in blobs.py to upload/download to/from an azure storage container._

## Setup/Installation Requirements

A [reddit account + application](https://www.reddit.com/) and [azure storage account + container](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal) are required to run this code. Also make sure you have a text editor such as Visual Studio Code installed, a python3.8 virtual environment active, and linux bash terminal to use
* _Clone this repository (https://github.com/jarretjeter/reddit-scraper.git) onto your local computer from github_
* _In VS Code or another text editor, open this project_
* _With your terminal, enter the command 'pip install -r requirements.txt' to get the necessary dependencies_
* _You will have to create environment variables saved to your reddit application personal use script (REDDSCRP_PU_SCRIPT), secret token (REDDSCRP_SECRET) and azure storage account connection string (REDDIT_STUFF_CONN_STR)_
* _Create a directory named "data" to save any files to_
* _Once everything is set up, in the command line you can run the scraper.py functions (for example: "python reddit.py threads [subreddit] [subject] --csv=true" or "python reddit.py comments [subreddit] [subject] --csv=true") to retrieve data from reddit_
* _For blobs.py, you can run python blobs.py containers to list all the containers in your storage account. Run "python blobs.py upload [filename] [container_name]" or "python blobs.py download [filename] [container_name]" to upload/download to or from your storage container._


## Known Bugs

* _No known bugs. Current limitation is that praw does not get ALL search results as if you were using a web browser_

## License
[MIT](https://github.com/jarretjeter/reddit-scraper/blob/main/LICENSE.txt)

Let me know if you have any questions at jarretjeter@gmail.com

Copyright (c) 10/4/2022 Jarret Jeter
