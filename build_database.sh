sort -u -o scores-sorted.txt scores.txt
sort -u -o pterms-sorted.txt pterms.txt
sort -u -o rterms-sorted.txt rterms.txt
perl break.pl < scores-sorted.txt > scores-final.txt
perl break.pl <pterms-sorted.txt >pterms-final.txt
perl break.pl <rterms-sorted.txt >rterms-final.txt
perl break.pl <reviews.txt >reviews-final.txt

db_load -c duplicates=1 -c dupsort=1 -f scores-final.txt -T -t btree sc.idx
db_load -c duplicates=1 -c dupsort=1 -T -t btree -f pterms-final.txt pt.idx
db_load -c duplicates=1 -c dupsort=1 -T -t btree -f rterms-final.txt rt.idx
db_load -T -t hash -f reviews-final.txt rw.idx