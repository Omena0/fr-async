#!/usr/bin/env zsh

source ~/.zshrc
setopt aliases

pytj -m fr_async.examples.http &
server_pid=$!

sleep 1

(
    cd ~/Github/http-c || exit 1
    ./bench -c 32 -j 8 -t 30 -P /README.md
)

kill "$server_pid"
wait "$server_pid" 2>/dev/null
