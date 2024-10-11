# Hi-res-traffic(high resolution traffic)

Process traffic signal high resolution data for troubleshooting and metrics. The data resolution is 0.1 sec and saved in one hour chunks to hard disk.

## API

api to process and retrieve hi-res data as needed for front end UI.

## UI

- Ag grid to view and filter hi-res data.

## Setup

App is built from a html/js frontend with bulma css. The backend is built with fastapi api with a postgress db. The entire app is containerized with docker.

Existing hi-res data is saved in a directory on network by atms system.  
Today only 30 days of data is saved at one time (old data overwritten).

Docker volume must be created becasue we must read from a shared windows drive, see cmd below:

```
docker volume create --driver local --opt type=cifs --opt device=//<server_ip>/<Path/To/Folder> --opt o=user=<user>,domain=<optional_domain>,password=<password> <name_of_volume>
```

use volume in docker run cmd:

```
docker run --rm -it -v <name_of_volume>:</directory/inContainer/toMountTo> --env-file .env -p 80:80 -p 8000:8000 hires_img
```

## TODO

[ ] Add timeline visual
[ ] Highlight critical faults like mmu flash, stop time, etc  
[ ] Allow user to make table or visual full screen  
[ ] **Metrics from hi-res**

- [ ] Able to select multilple intersectins or corridor for analysis
- [ ] Splits per cycle
- [ ] Avg split for phases over selected time range
- [ ] Avg trans time over selected time range
- [ ] Number of preempts
