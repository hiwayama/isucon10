#!/bin/bash -ex
# サービス再起動

cp ./db/0_Schema.sql ./../mysql/db/
cp ./db/3_AlterData.sql ./../mysql/db/

bundle install

sudo systemctl daemon-reload
sudo systemctl restart isuumo.ruby.service
sudo systemctl status isuumo.ruby.service
