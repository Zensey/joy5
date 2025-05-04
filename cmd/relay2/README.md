## About

The main aim of relay2 project is 24/7 endless broadcast of a music stream from
the Icecast2 broadcast server accompanied by a looped video. All is done w/o transcoding, so
the hardware requirements are low.

## Usage:

    ./relay2 -url=rtmp://a.rtmp.youtube.com/live2/ -acc-stream=http://myserver:8000/mystream.aac -key=1111-1111-1111-1111-1111,2222-2222-2222-2222-2222


# Development
## DONE:
* roll-over time 24h (for telegram it is 4h)
* video-pub: add delta to time until a full duration of the video and sleep(delta) 
* audio-pub: correct the timeline in case of reconnect (autocorrection)
* on sub reconnect - restart A/V sources
* on sub reconnect - close the remaining down-streams
* on restart A/V - rollover, reset subs state (accumulated packets)

## TODO:
* on restart A/V - rollover, reconnect subs as well
* use empty packet if no audio stream is available
