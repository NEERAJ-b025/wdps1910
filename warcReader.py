# WARC Reader which extracts warc records and strips html tags to fetch entities, stored into another file
# Also check and remove non english words using nltk
import warc
f = warc.WARCFile("data/sample.warc.gz", "rb")
i = 0
for record in f:
    print (record)
    i = i + 1
    if (i == 2):
        break