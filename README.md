# Lumen Cohort Builder

## Installation

### User

TBD

### Developer

Install [pixi](https://pixi.sh/) first, then run the following in your shell to create the
environment, install the `pre-commit` hooks, and finally install the package for development.

```bash
pixi shell --environment dev
pre-commit install
pip install --editable .
```

`pixi` will install the `gcloud` CLI (command line interface). You can follow the instructions on
how to authenticate with Google Cloud using their documentation here
[https://cloud.google.com/sdk/docs/install](https://cloud.google.com/sdk/docs/install). If you have
installed the dev environment using the above instructions, then you can run the following command
in your terminal to authenticate with Google.

```bash
gcloud init
```

After running the above command, you will be redirected to your default browser to sign in to your
Google Cloud account. Follow the instructions in your browser to log in. Logging in using your
browser will create default authentication files that will be used on subsequent logins. These
default files are used by Lumen Cohort Builder to authenticate to your Google Cloud account. If at
any point in time you need to **manually authenticate**, then you can run the following in your
terminal before running Lumen Cohort Builder.

```bash
gcloud auth application-default login
```

## Running

Execute the following command in your terminal to run the Lumen Cohort Builder.

```bash
panel serve src/lumen_cohort_builder/main.py --show
```

You will be automatically redirected to your default browser, where Lumen Cohort Builder will open
in a new tab.
