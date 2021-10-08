sudo rm -r /usr/lib/libunikv.so*
sudo rm -r /usr/local/lib/libunikv*
autoreconf -i
./configure
make clean
make -j8
sudo make install
sudo ldconfig
