FILE=$1

head -n 4 ${FILE}.txt > ${FILE}2.txt
tail -n 1 ${FILE}2.txt > ${FILE}.txt
tail -c +6 ${FILE}.txt > ${FILE}2.txt
sed 's/..$//' ${FILE}2.txt > ${FILE}.txt

perl -pe 's/\\\\n/STUPID_TEMP/g' ${FILE}.txt > ${FILE}.sed
perl -pe 's/\\n/\n/g' ${FILE}.sed > ${FILE}.sed
perl -pe 's/\\\\n/STUPID_TEMP/g' ${FILE}.txt > ${FILE}.sed
perl -pe 's/\\n/\n/g' ${FILE}.sed > ${FILE}2.sed
perl -pe 's/STUPID_TEMP/\\n/g' ${FILE}2.sed > ${FILE}.sed
perl -pe 's/\\\"/\"/g' ${FILE}.sed > ${FILE}2.sed
perl -pe 's/\\\\/\\/g' ${FILE}2.sed > ${FILE}.sed
dot -Tpdf < ${FILE}.sed > ${FILE}.pdf

rm ${FILE}.sed
rm ${FILE}2.sed

