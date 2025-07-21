echo 'sample,fastq_1,fastq_2' > samplesheet.csv
while read line; do
    name=$(echo $line | cut -d ' ' -f 1)
    code=$(echo $line | cut -d ' ' -f 2)
    r1=$(find /grphome/grp_geode/raw_sequences/20kb/plate${1} -iname "*$code*_R1_*")
    r2=$(find /grphome/grp_geode/raw_sequences/20kb/plate${1} -iname "*$code*_R2_*")
    echo "$name,$r1,$r2"
done < input.txt >> samplesheet.csv
