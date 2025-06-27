#!/bin/bash

export REPO_PATH=
export DB_PATH=
export GH_AUTH_TOKEN=
export GH_PROJECT_OWNER=
export GH_PROJECT_NAME=

# To compute all VCS related metrics:
prime vcs --input $REPO_PATH --output $DB_PATH && \
prime filesize --input $REPO_PATH --output $DB_PATH && \
prime project-size --output $DB_PATH && \
prime project-productivity --output $DB_PATH && \
prime bus-factor --output $DB_PATH

# To compute all project issue tracker related metrics:
prime issues --auth $GH_AUTH_TOKEN --owner $GH_PROJECT_OWNER --name $GH_PROJECT_NAME --output $DB_PATH && \
prime issue-spoilage --output $DB_PATH
prime issue-density --output $DB_PATH

# To compute all pull request tracker related metrics:
prime pull-requests --auth $GH_AUTH_TOKEN --owner $GH_PROJECT_OWNER --name $GH_PROJECT_NAME --output $DB_PATH && \
prime pull-request-spoilage --output $DB_PATH
