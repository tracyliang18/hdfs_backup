#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
from datetime import date,timedelta
import datetime
import re

date_re=re.compile("\d{4}-\d{2}-\d{2}$")


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

    ret = os.system("hadoop dfs -ls {0} > {1} 2>/dev/null".format(d,"containDate.tmp"))
    if ret != 0:
        print "[ERROR] please ensure that the hdfs path {0} exists\n".format(d)
        return False
    with open("containDate.tmp") as f:
        date_str = []
        for line in f:
            fields = line.strip().split()
            ftype = fields[0]
            fname = fields[-1]
            #print ftype,fname
            if is_dir(ftype) and date_re.search(fname):
                date_str.append(fname)
        if len(date_str) > 0:
            return date_str
        else:
            print "[ERROR] {0} doesn't include date subdir\n".format(d)
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
    if not start_date_str:
        start_date_str = "1970-01-01"

    if not end_date_str:
        end_date_str = "2100-01-01"

    pwd = os.path.dirname(os.path.realpath(__file__))
    os.system("mkdir {backup_root}".format(backup_root=backup_type))
    ret = containDate(src_dir)
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    start_date = datetime.datetime.strptime(start_date_str,"%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date_str,"%Y-%m-%d")

    success_list = []
    failed_list = []
    if ret:
        #print ret
	ret.sort()
	ret_s = ret[0].split("/")[-1]
	ret_e = ret[-1].split("/")[-1]
	print "backup dir: {0}".format(src_dir)
	#print "min_date: {0}, max_date: {1}".format(ret_s,ret_e)
	print "min_date :  {0}, max_date : {1}".format(date_re.search(ret_s).group(0), date_re.search(ret_e).group(0))
	for date_dir in ret:
            name = date_dir.split("/")[-1]
            file_date_str = date_re.search(name).group(0)
	    file_date = datetime.datetime.strptime(file_date_str,"%Y-%m-%d")
            #print file_date
	    if file_date <= end_date and file_date >= start_date:
            	ret1 = os.system("hadoop dfs -get {src} {backup_root}/{date} 2>/dev/null".format(src=date_dir,backup_root=backup_type,date=file_date_str))

                if ret1 != 0:
                    print "[ERROR] hadoop dfs get file {src} failed".format(src=date_dir)

                else:

		    #print "hadoop dfs -cat {src}/* 1>{backup_root}/{date} 2>/dev/null".format(src=date_dir,backup_root=backup_type,date=file_date_str)
            	    ret2 = os.system("cd {backup_root} && tar -czvf {date}.tar.gz {date} 2>/dev/null".format(backup_root=backup_type, date=file_date_str))
                    if ret2 != 0:
                        print "[ERROR] tar {backup_root}/{date}.tar.gz failed".format(backup_root=backup_type,date=file_date_str)

                    else:
                        ret3 = os.system("scp {backup_root}/{date}.tar.gz {remote}".format(backup_root=backup_type,date=file_date_str,remote=remote_dir))
                        if ret3 != 0:
                            print "[ERROR] scp {backup_root}/{date}.tar.gz error".format(backup_root=backup_type,date=file_date_str)
                    os.system("rm -rf {backup_root}/{date} {backup_root}/{date}.tar.gz".format(backup_root=pwd + "/" + backup_type,date=file_date_str))
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
        print "{0} backup failed, please ensure that {0} contains %Y-%m-%d subdir".format(src_dir)

    if gen_delete_script and len(success_list) > 0:

        with open("delete_script_{0}_{1}.sh".format(backup_type,today),"w") as f:
            cmd_format = "hadoop dfs -rmr -skipTrash {filename}"
            for fname in success_list:
                f.write(cmd_format.format(filename=fname + "\n"))

        print "generate delete script completed(only those backuped successfully would be added to script)"
    os.system("rm -r {backup_root}".format(backup_root=pwd + "/" + backup_type))
    return

#def main():
#    if

if __name__ == "__main__":
    #test_containDate()
    backup_given_date("xtx_tracking","/user/hive/external/tw_tr_datas_xtx_d","root@10.0.0.172:/home/ubuntu/backup/hdfs/test/","2015-08-01","2015-09-01",True)

    #backup_given_date("xtx_tracking","/user/hive/external/tw_tr_datas_xtx_d",start_date_str="2015-11-01")
    #backup_given_date("xtx_tracking","/user/hive/external/tw_tr_datas_xtx_d",start_date_str="2015-11-01",end_date_str="2015-11-03")


