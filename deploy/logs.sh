#!/bin/bash

set -x

yarn application -kill $1

set -x
yarn logs -applicationId $1 > awise.log
