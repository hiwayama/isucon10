#!/bin/bash
# サービス再起動

sudo systemctl daemon-reload
sudo systemctl restart isuumo.ruby.service
sudo systemctl status isuumo.ruby.service
