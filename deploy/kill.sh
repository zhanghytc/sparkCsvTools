#!/bin/bash

yarn application -kill  $1 
yarn logs -applicationId $1  > k.log
