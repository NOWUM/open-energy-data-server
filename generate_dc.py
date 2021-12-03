#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# generates a docker-compose.yml to crawl the data from dwd

output = []
output.append('version: "3"\n')
output.append('services:\n')


i = 0
for year in range(1995,2016):
    i +=1
    output.append(f'''
  dwd{i}:
    container_name: dwd{i}
    image: dwd_crawler:latest
    build:
      context: .
      dockerfile: ./dockerfiles/Dockerfile_dwd
    environment:
      START_DATE: {year}01
      END_DATE: {year}12
      ''')

with open('docker-compose.yml', 'w') as f:
  f.writelines(output)
