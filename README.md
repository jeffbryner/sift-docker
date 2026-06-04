# sift-ai
A docker build of the sift tools for use by ai

`docker build -t sift-ai -f Dockerfile.ai .  `

# Space management
Sift/cast is BIG. Ensure you have space for the build. 

```bash
docker builder prune -a
```

Using colima? 
Give it build space when starting. 
```colima start --disk 120```

Based on :
# sift-docker  
A SANS SIFT Docker built on Ubuntu 20.04 from https://github.com/digitalsleuth/sift-docker

Rebuilt specifically for use by AI as a container to run docker exec commands in.