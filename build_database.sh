sort -u -o scores-sorted.txt scores.txt
sort -u -o pterms-sorted.txt pterms.txt
sort -u -o rterms-sorted.txt rterms.txt
perl break.pl <scores-sorted.txt >scores-final.txt
perl break.pl <pterms-sorted.txt >pterms-final.txt
perl break.pl <rterms-sorted.txt >rterms-final.txt
perl break.pl <reviews.txt >reviews-final.txt

if [ -n "$1" ]; then 
    echo "Database given: $1"
    db_load

else echo "No database argument given."
fi