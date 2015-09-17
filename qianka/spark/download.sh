i=0
while((i<20))
do
    aws s3 cp s3://logarchive.ym/user_packages/1509/ios/youmi_ios_user_packages_${i}_5.txt.gz ./package_install
    echo youmi_ios_user_packages_${i}_5.txt.gz
    i=`expr $i + 1`
done
