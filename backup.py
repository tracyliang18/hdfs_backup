#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
from datetime import date,timedelta
import datetime
import re


#support regex
date_range_re=re.compile("\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}")
date_re=re.compile("\d{4}-\d{2}-\d{2}")
support_date_format = [date_range_re, date_re]

today = datetime.datetime.now().strftime("%Y-%m-%d")


def containDate(d):
    """
        judge whether directory d contain date-format subdir, and return all date-format sub-directorys or subfile in a list
    """
    containDate.index += 1
    tmp_file = "containDate.tmp" + str(containDate.index)
    if not d.endswith("/"):
        d += "/"
    root = d
    ret = os.system("hadoop dfs -lsr {0} > {1} 2>/dev/null".format(root,tmp_file))
    if ret != 0:
        print "[ERROR] please ensure that the hdfs path {0} exists\n".format(root)
        return False
    date_str = []
    with open(tmp_file) as f:

        item_set = set()
        for line in f:
            fields = line.strip().split()
            ftype = fields[0]
            fname = fields[-1]
            children = fname[len(root):]
            #print "children",children
            for regex in support_date_format:
                match = regex.search(children)
                if match:
                    prefix_end = children.rfind("/",0,match.start())
                    if prefix_end == -1:
                        prefix_end = 0
                    prefix = children[0:prefix_end]
                    end = children.find("/",match.end())
                    if end == -1:
                        end = len(children)
                    backup_item = root + children[0:end]
                    if backup_item not in item_set:
                        date_str.append((prefix,backup_item))
                        item_set.add(backup_item)
                    break
    os.system("rm {f}".format(f=tmp_file))
    if len(date_str) > 0:
        #print "[DEBUG] total backup item count : {0}".format(len(date_str))
        return date_str
    else:
        print "[ERROR] {0} doesn't include date subdir\n".format(root)
        return False


containDate.index = 0


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def backup_given_date(backup_type,src_dir,remote_dir,start_date_str,end_date_str,gen_delete_script):
    """
        backup the sub-directorys in d which are created before end_date_str

        backup_type: string,备份类型,用于创建临时目录
        src_dir : string,需要备份的hdfs目录
        remote_dir : string,保存备份的远程目录
        start_date_str : string,日期,格式为%Y-%m-%d
        end_date_str : string,日期,格式为%Y-%m-%d
        gen_delete_script : bool,是否生成删除脚本

    """

    remote_host=remote_dir.split("/")[0].strip(":")
    remote_base_dir="/" + "/".join(remote_dir.split("/")[1:]) + "/"



    if not start_date_str:
        start_date_str = "1970-01-01"

    if not end_date_str:
        end_date_str = "2100-01-01"

    pwd = os.path.dirname(os.path.realpath(__file__))
    os.system("mkdir {backup_root}".format(backup_root=backup_type))
    ret = containDate(src_dir)
    start_date = datetime.datetime.strptime(start_date_str,"%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date_str,"%Y-%m-%d")


    success_list = []
    failed_list = []
    if ret:
        #print ret
        ret.sort()

        prefix_list = []
        pre = ""
        i = 0
        end = len(ret)
        mark = ret[0][0]

        while i < end:
            #print ret[i][0]
            min_date = ret[i][1].rstrip("/").split("/")[-1]
            while(i < end and ret[i][0] == mark):
                i += 1
            max_date = ret[i-1][1].rstrip("/").split("/")[-1]
            prefix_list.append((mark,min_date,max_date))
            if i < end:
                mark = ret[i][0]

        print "backup root dir: {0}".format(src_dir)
        print "backup {0} subdir".format(len(prefix_list))
        for e in prefix_list:
            print "\t dirname: {0}, min_date: {1}, max_date: {2}".format(e[0] if len(e[0])>0 else ".",e[1],e[2])

            #print "min_date: {0}, max_date: {1}".format(ret_s,ret_e)
            #print "min_date :  {0}, max_date : {1}".format(date_re.search(ret_s).group(0), date_re.search(ret_e).group(0))
        for prefix,date_dir in ret:
            #print prefix,date_dir
            name = date_dir.rstrip("/").split("/")[-1]
            file_date_str = re.search("\d{4}-\d{2}-\d{2}",name).group(0)
            #print file_date_str
            file_date = datetime.datetime.strptime(file_date_str,"%Y-%m-%d")
            #print file_date
            if file_date <= end_date and file_date >= start_date:
                ret1 = os.system("hadoop dfs -get {src} {backup_root}/{date} 1>/dev/null 2>&1".format(src=date_dir,backup_root=backup_type,date=name))

                if ret1 != 0:
                    print "[ERROR] hadoop dfs get file {src} failed".format(src=date_dir)

                else:

                    #print "hadoop dfs -cat {src}/* 1>{backup_root}/{date} 2>/dev/null".format(src=date_dir,backup_root=backup_type,date=file_date_str)
                    ret2 = os.system("cd {backup_root} && tar -czvf {date}.tar.gz {date} 1>/dev/null 2>&1".format(backup_root=backup_type, date=name))
                    if ret2 != 0:
                        print "[ERROR] tar {backup_root}/{date}.tar.gz failed".format(backup_root=backup_type,date=name)

                    else:

                        md5_command_ret1 = os.system("cd {backup_root} && md5sum {date}.tar.gz | awk '{{print $1}}' > {date}.md5_source_tmp".format(backup_root=backup_type,date=name))
                        #print "cd {backup_root} && md5sum {date}.tar.gz | awk '{{print $1}}' > {date}.md5_source_tmp".format(backup_root=backup_type,date=name)
                        ret3 = os.system("ssh {host} \"mkdir -p {base}/{prefix}\" && scp {backup_root}/{date}.tar.gz {remote}/{prefix}/ 1>/dev/null".format(host=remote_host,base=remote_base_dir,backup_root=backup_type,date=name,remote=remote_dir,prefix=prefix))

                        #print "ssh {host} \"md5sum {base}/{prefix}/{date}.tar.gz | awk '{{print $1}}' > /tmp/{date}.md5_remote_tmp\" && cd {backup_root} && scp {host}/tmp/md5_remote_tmp .".format(host=remote_host,base=remote_base_dir,prefix=prefix,date=name,backup_root=backup_type)
                        md5_command_ret2 = os.system("ssh {host} \"md5sum {base}/{prefix}/{date}.tar.gz | awk '{{print $1}}' > /tmp/{date}.md5_remote_tmp\" && cd {backup_root} && scp {host}:/tmp/{date}.md5_remote_tmp ."
                                                .format(host=remote_host,base=remote_base_dir,prefix=prefix,date=name,backup_root=backup_type))

                        if md5_command_ret1 != 0:
                            print "md5sum local file failed"

                        if md5_command_ret2 != 0:
                            print "md5sum remote file failed"
                        ret_diff = 1
                        if md5_command_ret1 == 0 and md5_command_ret2 == 0:
                            try:
                                with open("{backup_root}/{date}.md5_remote_tmp".format(backup_root=backup_type,date=name)) as f1, open("{backup_root}/{date}.md5_source_tmp".format(backup_root=backup_type,date=name)) as f2:
                                    #print "md5"
                                    #print f1.readlines()[0].split()[0],f2.readlines()[0].split()[0]
                                    if f1.readlines()[0].split()[0] == f2.readlines()[0].split()[0]:
                                        ret_diff = 0
                            except Exception as e:
                                print e
                                print "diff md5 result exception"

                        if ret_diff:
                            print "[ERROR] md5sum is different between two file in {date}.tar.gz in {backup_root}".format(date=name,backup_root=backup_type)

                        if ret3 != 0:
                            print "[ERROR] scp {backup_root}/{prefix} {date}.tar.gz error".format(backup_root=backup_type,date=name,prefix=prefix)
                os.system("rm -rf {backup_root}/{date} {backup_root}/{date}.tar.gz 1>/dev/null".format(backup_root=pwd + "/" + backup_type,date=name))

                if ret1 == 0 and ret2 == 0 and ret3 == 0 and ret_diff == 0:
                    success_list.append(date_dir)
                else:
                    failed_list.append(date_dir)

                    #print "backup {src} success".format(src=date_dir)
        print "[FINISH]{0} backup finish".format(src_dir)
        print "total processed {0}".format(len(success_list) + len(failed_list))
        print "backup succeed {0}".format(len(success_list))
        for s in success_list:
            print "\t[SUCCESS] ",s
        print "backup failed {0}".format(len(failed_list))
        for f in failed_list:
            print "\t[FAILED] ",f

    else:
        print "{0} backup failed, please ensure that {0} contains %Y-%m-%d subdir or subfile".format(src_dir)

    if gen_delete_script and len(success_list) > 0:

        with open("./delete_script/delete_script_{0}_{1}.sh".format(backup_type,today),"w") as f:
            cmd_format = "hadoop dfs -rmr {filename}"
            for fname in success_list:
                f.write(cmd_format.format(filename=fname + "\n"))

        print "generate delete {0} script completed(only those backuped successfully would be added to script)".format(backup_type)
    os.system("rm -r {backup_root}".format(backup_root=pwd + "/" + backup_type))
    return

#def main():
#    if

if __name__ == "__main__":
    #test_containDate()
    backup_given_date("xtx_tracking","/user/hive/external/tw_tr_datas_xtx_d","root@10.0.0.172:/home/ubuntu/backup/hdfs/test/","2015-08-01","2015-09-01",True)

    #backup_given_date("xtx_tracking","/user/hive/external/tw_tr_datas_xtx_d",start_date_str="2015-11-01")
    #backup_given_date("xtx_tracking","/user/hive/external/tw_tr_datas_xtx_d",start_date_str="2015-11-01",end_date_str="2015-11-03")
