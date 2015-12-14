#backup "source.comments", "source.tracking", "source.mobile" "interatcion" "datas" "app" under /production/ in hdfs,  keep 90 days data
set -v
three_month_backup_list=("source.comments" "source.tracking" "source.mobile" "interatcion" "datas" "app")

#backup "source.mysql-bak" under /production/ in hdfs,  keep one week data
one_week_backup_list=("source.mysql-bak")

if [ ! -d "out"]; then
  mkdir out
fi


for bk_item in ${three_month_backup_list[@]}
do
  python run.py -k 90 -t all.${bk_item} -d > out/${bk_item}_out
done

for bk_item in ${one_week_backup_list[@]}
do
  python run.py -k 7 -t all.${bk_item} -d > out/${bk_item}_out
done
