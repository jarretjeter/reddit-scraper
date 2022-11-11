# reddit-scraper

#### By Jarret Jeter

#### Reddit web-scraper project using the Python Reddit API Wrapper (praw) and the Python Pushshift.io API Wrapper (psaw) for the general purpose of getting information on user comments on a particular subject.

## Technologies Used

* _Azure Storage_
* _Python_
* _Pandas_
* _Praw_
* _Psaw_
* _Typer_

## Description

_With Python's [Praw](https://github.com/praw-dev/praw) package I instantiated a Reddit object using a Reddit application to be used by the [PushShift API Wrapper](https://github.com/dmarx/psaw). The Reddit object can access many things through Reddit's API such as subreddits, users, topics, and comments. I defined some functions to structure submission and comment data csv's as I like from Reddit, and merging multiple batches of data to a csv if required. I then have some functions defined in blobs.py to upload/download to/from an azure storage container._

## Setup/Installation Requirements

A [reddit account + application](https://www.reddit.com/) and [azure storage account + container](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal) are required to run this code. Also make sure you have a text editor such as Visual Studio Code installed, a python3.8 virtual environment active, and linux bash terminal to use
* _Clone this repository (https://github.com/jarretjeter/reddit-scraper.git) onto your local computer from github_
* _In VS Code or another text editor, open this project_
* _With your terminal, enter the command 'pip install -r requirements.txt' to get the necessary dependencies_
* _You will have to create environment variables saved to your reddit application personal use script (REDDSCRP_PU_SCRIPT), secret token (REDDSCRP_SECRET) and azure storage account connection string (REDDIT_STUFF_CONN_STR)_
* _Create a directory named "data" to save any files to_
* _Once everything is set up, in the command line you can run the scraper2.py functions (for example: "python scraper2.py fetch\_threads [subreddit] [subject] or "python scraper2.py fetch\_comments [subreddit] [subject]") to retrieve data from reddit_
* _For blobs.py, you can run python blobs.py containers to list all the containers in your storage account. Run "python blobs.py upload [filename] [container_name]" or "python blobs.py download [filename] [container_name]" to upload/download to or from your storage container._


## Known Bugs

* _No known bugs._

## License
[MIT](https://github.com/jarretjeter/reddit-scraper/blob/main/LICENSE.txt)

Let me know if you have any questions at jarretjeter@gmail.com

Copyright (c) 11/11/2022 Jarret Jeter
