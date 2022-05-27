#!/bin/bash
sudo rm -r /usr/lib/libunikv.so*
sudo rm -r /usr/local/lib/libunikv*
sudo cp out-shared/libunikv.so* /usr/local/lib &sudo cp out-shared/libunikv.so* /usr/lib &sudo cp -R include/* /usr/local/include & sudo cp out-static/libunikv.a /usr/local/lib
