#!/bin/bash

awk 'NR % 2 == 0' shakespeare.json \
| jq -c 'select(.line_number != "")
        | {
            "play":.play_name,
            "lineCoordinates":.line_number,
            "speaker": .speaker,
            "text": .text_entry
        }' \
> ./src/test/resources/shakespear-cleaned.json