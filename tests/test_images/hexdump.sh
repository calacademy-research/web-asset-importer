#!/bin/bash
xxd -p test_image.jpg > test_hexdump.txt
xxd -r -p test_hexdump.txt | LC_ALL=C tr -d -c .ICH_SCAN_FOLDERS[:print:]' > ascii_hex.txt

