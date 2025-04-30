#!/usr/bin/env bash

set -e

# error prints an error message and exits. builds a payload that can be used to create a GitLab release via the API.
# arguments: [Message...]
error() {
  printf "\n"
  printf "%s" "$*" >>/dev/stderr
  printf "\n"
  exit 1
}

# log prints a message in a heading format.
# arguments: [Message...]
log() {
  printf "\n\n######### %s #########\n" "$*" >>/dev/stdout
}

# debug_log prints a message.
# arguments: [Message...]
debug_log() {
  printf "\n\n# %s\n" "$*" >>/dev/stdout
}

# verify_has_value errors with given message if the variable is not set.
# arguments: [Variable] [Message]
verify_has_value() {
  variable="$1"
  message="$2"

  if [[ -z $variable ]]; then
    error "$message"
  fi
}

# error_if_has_value errors with value of the first argument if the first argument has a non-empty value.
# arguments: [Variable]
error_if_has_value() {
  variable="$1"

  if [[ -n $variable ]]; then
    error "$variable"
  fi
}

retry() {
  timeout="$1"
  command="$2"

  tmpout="$(mktemp)"
  endtime=$(gdate -ud "$timeout" +%s)
  result=0
  echo "Trying until $endtime..."
  while [[ $(gdate -u +%s) -le $endtime ]]
  do

    rm -f "$tmpout"

    set +e
    bash -c "$command" >> "$tmpout" 2>&1
    result=$?
    set -e

    if [[ $result -eq 0 ]]; then
      break
    fi

   sleep 0.2
  done
  if [[ $result -ne 0 ]]; then
    cat "$tmpout"
    echo "Command did not succeed within the required $timeout: '$command'"
    exit 1
  fi
  rm -f "$tmpout"
}
