## hdfs backup tools                                                                                                                                                                                                   ### usage:```Options:  -h, --help            show this help message and exit  -s YYYY-mm-dd, --start=YYYY-mm-dd                        backup start date, in %Y-%m-%d format  -e YYYY-mm-dd, --end=YYYY-mm-dd                        backup end date, in %Y-%m-%d format  -k int, --keep=int    keep only the most recent k day file, backup the old                         file(-s and -t options would be ignored)  -c conf.json, --conf=conf.json                        config file, setting the backup hdfs-src path and                         remote target path  -t AAA.BBB.CCC, --type=AAA.BBB.CCC                        backup data type, should be in XX.XXX.XX format, such                        as source.tracking.xtx```### example:1. backup the source.tracking.xtx data between 2015-09-01 to 2015-10-01```python run.py -s 2015-09-01 -e 2015-10-01 -t source.tracking.xtx```2. 只保留最近10天 ,其余拉走备份```python run.py -k 10 -t source.tracking.xtx```3. 备份截止到2015-10-10 的数据```python run.py -e 2015-10-10 source.tracking.xtx```### about conf.json:conf.json 的叶子节点必须保存着hdfs-src,remote两个参数,分别代表备份源路径和备份目标路径. 备份类型为到达该叶子节点的键值,例如 source.tracking.xtx