## hdfs backup tools        

## 简介
hdfs 备份工具,会把生产环境的hdfs数据(天为单位)备份到存储机器(现10.0.0.147)

备份时需要设置备份类型,需要备份的日期.


## 备份类型配置
在conf.json 配置备份树结构
若节点包含`hdfs-src`和`remote`,则为一个备份单元.分别代表备份源路径和远程目标路径

如在conf.json 定义了如下
```json
{
    "all" : {
        "source" : {
          "tracking" : {
            "hdfs-src" : "/production/source/tracking/",
            "remote" : "root@10.0.0.147:/root/backup/hdfs/production/source/tracking/"
          },

          "comments" : {
            "hdfs-src" : "/production/source/comments",
            "remote" : "root@10.0.0.147:/root/backup/hdfs/production/source/comments"
          }
        },

        "datas" : {
          "hdfs-src" : "/production/datas/",
          "remote" : "root@10.0.0.147:/root/backup/hdfs/production/datas",
        },


        "app" : {
          "hdfs-src" : "/production/app/",
          "remote" : "root@10.0.0.147:/root/backup/hdfs/production/app"
        },


        "interaction" : {
          "hdfs-src" : "/production/interaction/",
          "remote" : "root@10.0.0.147:/root/backup/hdfs/production/interaction"
        },

        "exceptions" : {
            "hdfs-src" : "/production/exceptions/",
            "remote" : "root@10.0.0.147:/root/backup/hdfs/production/exceptions"
        },

        "opjobs" : {
          "hdfs-src" : "/production/opjobs",
          "remote" : "root@10.0.0.147:/root/backup/hdfs/production/opjobs/"
        }
}
```


## 备份细节
对于备份类型配置数conf.json,程序会以其配置的`hdfs-src`作为搜索根路径,并寻找以`%Y-%m-%d`格式的文件或目录,以此作为打包的文件.

如`hdfs-src=/root`,`remote=/remote/backup/`,root下有六个目录(或文件)
```
/root/2015-11-01
/root/2015-11-02
/root/2015-11-03/XXX/2015-11-03
/root/haha/2015-11-01
/root/haha/2015-11-02
/root/wawa
```
则有前五个目录(文件)将会备份,备份的结果为
```
/remote/backup/root/2015-11-01.tar.gz
/remote/backup/root/2015-11-02.tar.gz
/remote/backup/root/2015-11-03.tar.gz
/remote/backup/root/haha/2015-11-01.tar.gz
/remote/backup/root/haha/2015-11-02.tar.gz
```
需要注意的是第三个备份文件,当路径中有多处符合日期格式时,只会以最外层作为备份打包单位


### 例子:
1. backup the source.tracking data between 2015-09-01 to 2015-10-01

```
python run.py -s 2015-09-01 -e 2015-10-01 -t all.source.tracking
```
2. 只保留source最近10天 ,其余拉走备份

```
python run.py -k 10 -t source
```
3. 备份截止到2015-10-10 的datas.tracking数据

```
python run.py -e 2015-10-10 all.data.tracking
```

4. 在备份同时生成删除脚本(-d)

```
python run.py -s 2015-10-01 -e 2015-11-01 -t XX.XX.XX -d
```

5. 如下命令代表执行source.tracking备份,程序会搜索source.tracking指定的hdfs-src路径下的所有日期文件或目录,并以最外面一层作为备份压缩单位

```
python run.py -s 2015-11-11 -e 2015-11-11 -t all.source.tracking 
```

6. 备份类型可以不为叶子节点,此时备份类型为指定节点下的所有叶子节点,如:

```
python run.py -k 30 -t all.source
```

### usage:
```
Options:
  -h, --help            show this help message and exit
  -s YYYY-mm-dd, --start=YYYY-mm-dd
                        backup start date, in %Y-%m-%d format
  -e YYYY-mm-dd, --end=YYYY-mm-dd
                        backup end date, in %Y-%m-%d format
  -k int, --keep=int    keep only the most recent k day file, backup the old
                        file(-s and -t options would be ignored)
  -c conf.json, --conf=conf.json
                        config file, setting the backup hdfs-src path and
                        remote target path
  -t AAA.BBB.CCC, --type=AAA.BBB.CCC
                        backup data type, should be in XX.XXX.XX format, such
                        as source.tracking.xtx
  -d, --delete          switch on generate delte script

```

### about conf.json:

conf.json 的叶子节点<strong>必须</strong>保存着`hdfs-src`,`remote`两个参数,分别代表备份源路径和备份目标路径.


备份类型为json中键值,例如 `all.source.tracking`, `all.source`, `all.datas`, `all` 都为合法键值

