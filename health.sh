#!/bin/bash
# 動くかチェック
curl localhost:1323/
curl localhost:1323/api/chair/low_priced
curl -X POST localhost:1323/initialize