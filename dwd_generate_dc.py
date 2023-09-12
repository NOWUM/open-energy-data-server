#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generates a docker-compose.yml to crawl the data from dwd.

The data from DWD has a lot of data stored in grb files.
To extract and aggregate the data efficiently, the load is divided onto multiple containers.
This allows to download all the data faster, if one wants to mirror all of DWDs history.
Still, this takes a few days to finish, with one container per download year.
"""
output = []
output.append('version: "3"\n')
output.append("services:\n")

i = 0
for year in range(1995, 2019):
    i += 1
    output.append(
        f"""
  dwd{i}:
    container_name: dwd{i}
    image: registry.git.fh-aachen.de/nowum-energy/projects/fh-opendata/dwd_crawler:latest
    environment:
      START_DATE: {year}01
      END_DATE: {year}12
      """
    )

with open("dwd_docker-compose.yml", "w") as f:
    f.writelines(output)
