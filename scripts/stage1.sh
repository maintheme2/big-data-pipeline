#!/bin/bash
url="https://disk.yandex.ru/d/WfsUS2K8esyCrA"

rm -f data/accidents.csv

wget "$(yadisk-direct $url)" -O data/accidents.csv