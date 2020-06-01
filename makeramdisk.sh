
#!/bin/bash

mkdir /mnt/ramdisk

mount -t tmpfs -o rw,size=2G tmpfs /mnt/ramdisk
