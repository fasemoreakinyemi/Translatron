#!/bin/sh
#rsync --checksum --partial --progress -av ftp.ncbi.nlm.nih.gov::pub/pmc/articles* .
rsync -Pav "rsync://ftp.ncbi.nlm.nih.gov/pub/pmc/articles.*" .
