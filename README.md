
initial thoughts:

- handle late events. either wait for last event of the previous day or keep track of which minutes were processed. for that purpose, it's worth realizing if the events are always in order.
- can we be inaccurate?.. if so, use HLL or TS. otherwise, we need to keep track of all activities & modules & users per day. we can use actual list if there are relatively few, but better use a bitmap.
- might worth thinking about compaction? (7 days can become one single "week" entry once the data is x days old, if we don't care about historical precision down to a single day granularity)
