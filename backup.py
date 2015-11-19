#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
from datetime import date,timedelta
import datetime
import re

date_re=re.compile("\d{4}-\d{2}-\d{2}")
today = datetime.datetime.now().strftime("%Y-%m-%d")

def is_dir(d):
    """
        judge whether d is directory
    """

    if d.startswith("d"):
        return True
    return False

def containDate(d):
    """
        judge whether directory d contain date-format subdir
    """
    if not d.endswith("/"):
        d += "/"
    root = d
    ret = os.system("hadoop dfs -lsr {0} > {1} 2>/dev/null".format(root,"containDate.tmp"))
    if ret != 0:
        print "[ERROR] please ensure that the hdfs path {0} exists\n".format(root)
        return False
    with open("containDate.tmp") as f:
        date_str = []
        item_set = set()
        for line in f:
            fields = line.strip().split()
            ftype = fields[0]
            fname = fields[-1]
            children = fname[len(root):]
            match = date_re.search(children)
            if match:
                prefix = children[0:match.start()]
                backup_item = root + children[0:match.end()]
                if backup_item not in item_set:
                    date_str.append((prefix,backup_item))
                    item_set.add(backup_item)
        if len(date_str) > 0:
            #print "[DEBUG] total backup item count : {0}".format(len(date_str))
            return date_str
        else:
            print "[ERROR] {0} doesn't include date subdir\n".format(root)
            return False
    os.system("rm containDate.tmp")


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def backup_given_date(backup_type,src_dir,remote_dir,start_date_str,end_date_str,gen_delete_script):
    """
        backup the sub-directorys in d which are created before end_date_str

        backup_type: 备份类型,用于创建临时目录
        src_dir : 需要备份的hdfs目录
        remote_dir : 保存备份的远程目录
        start_date_str : 日期,格式为%Y-%m-%d
        end_date_str : 日期,格式为%Y-%m-%d

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
            print ret[i][0]
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
            print "\t dirname: {0}, min_date: {1}, max_date: {2}".format(e[0],e[1],e[2])
        
            #print "min_date: {0}, max_date: {1}".format(ret_s,ret_e)
            #print "min_date :  {0}, max_date : {1}".format(date_re.search(ret_s).group(0), date_re.search(ret_e).group(0))
        for prefix,date_dir in ret:
            #print prefix,date_dir
            name = date_dir.rstrip("/").split("/")[-1]
            file_date_str = date_re.search(name).group(0)
            file_date = datetime.datetime.strptime(file_date_str,"%Y-%m-%d")
            #print file_date
            if file_date <= end_date and file_date >= start_date:
                ret1 = os.system("hadoop dfs -get {src} {backup_root}/{date} 1>/dev/null ".format(src=date_dir,backup_root=backup_type,date=file_date_str))

                if ret1 != 0:
                    print "[ERROR] hadoop dfs get file {src} failed".format(src=date_dir)

                else:

                    #print "hadoop dfs -cat {src}/* 1>{backup_root}/{date} 2>/dev/null".format(src=date_dir,backup_root=backup_type,date=file_date_str)
                    ret2 = os.system("cd {backup_root} && tar -czvf {date}.tar.gz {date} 1>/dev/null ".format(backup_root=backup_type, date=file_date_str))
                    if ret2 != 0:
                        print "[ERROR] tar {backup_root}/{date}.tar.gz failed".format(backup_root=backup_type,date=file_date_str)

                    else:

                        ret3 = os.system("ssh {host} \"mkdir -p {base}/{prefix}\" && scp {backup_root}/{date}.tar.gz {remote}/{prefix}/ 1>/dev/null".format(host=remote_host,base=remote_base_dir,backup_root=backup_type,date=file_date_str,remote=remote_dir,prefix=prefix))
                        if ret3 != 0:
                            print "[ERROR] scp {backup_root}/{prefix} {date}.tar.gz error".format(backup_root=backup_type,date=file_date_str,prefix=prefix)
                os.system("rm -rf {backup_root}/{date} {backup_root}/{date}.tar.gz 1>/dev/null".format(backup_root=pwd + "/" + backup_type,date=file_date_str))
                
                if ret1 == 0 and ret2 == 0 and ret3 == 0:
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

        with open("delete_script_{0}_{1}.sh".format(backup_type,today),"w") as f:
            cmd_format = "hadoop dfs -rmr -skipTrash {filename}"
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


