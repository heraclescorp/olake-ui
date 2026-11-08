#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

aws ecr get-login-password --region us-east-2 |
  docker login --username AWS --password-stdin 199658938451.dkr.ecr.us-east-2.amazonaws.com

docker build --platform linux/amd64 \
  --build-arg "APP_VERSION=git-$(git rev-parse --short=12 HEAD)" \
  --tag 199658938451.dkr.ecr.us-east-2.amazonaws.com/olakego/ui:latest .

docker push 199658938451.dkr.ecr.us-east-2.amazonaws.com/olakego/ui:latest
