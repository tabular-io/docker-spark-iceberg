#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

usage () {
  echo "usage: $0 [-t <image_tag>]"
  echo "  -t      Version tag of the image being pushed. Defaults to \"latest\""
  echo "  -v      Turn on VERBOSE / DEBUG output"
  exit 1
}

validate_tag_or_throw() {
  if [[ ! "$1" =~ ^([a-z0-9\_])([\-\.\_a-z0-9]{0,127})$ ]]; then
    echo "The image tag \"$1\" is invalid."
    echo "  A valid image tag must be valid ASCII and may contain lowercase and uppercase letters, digits, underscores, periods and dashes."
    echo "  A tag name may not start with a period or a dash and may contain a maximum of 128 characters"
    exit 1
  fi
}

# Default repository remote name
REPOSITORY="tabulario"
IMAGE_NAME="spark-iceberg"
TAG="latest"

while getopts "t:v" opt; do
  case "${opt}" in
    t)
      TAG="${OPTARG}"
      ;;
    v)
      set -x
      ;;
    *)
      usage
      ;;
  esac
done

shift $((OPTIND-1))

if [[ -z "$TAG" ]]; then
  echo "You must specify the image tag to be used via the -t switch. If left empty, then \"latest\" will be used"
  usage
fi

# Validate docker is installed
if ! command -v docker  &> /dev/null; then
  echo "The docker command could not be found. Please install docker and rerun."
  exit
fi

# Validate docker is on
if ! docker info > /dev/null 2>&1; then
  echo "The docker daemon is not running or accessible. Please start docker and rerun."
  usage
fi

# Validate the tag is acceptable
validate_tag_or_throw "$TAG"

# Build full image target and log.
IMAGE_TARGET=${REPOSITORY}/${IMAGE_NAME}:${TAG}

echo "Building and pushing multi-architecture image as ${IMAGE_TARGET} for platforms linux/amd64 and linux/arm64"

docker buildx build -t ${IMAGE_TARGET} --platform=linux/amd64,linux/arm64 ./spark --load
