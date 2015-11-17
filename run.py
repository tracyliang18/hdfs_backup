from optparse import OptionParser

#from 
parser = OptionParser()
parser.add_option("-s", "--start", dest="start_date_str",default=None,
                  help="backup start date, in %Y-%m-%d format", metavar="YYYY-mm-dd")
parser.add_option("-e", "--end", dest="end_date_str", default=None,
                  help="backup end date, in %Y-%m-%d format", metavar="YYYY-mm-dd")

parser.add_option("-k", "--keep", dest="keep", default = None, type="int",
	          help="keep only the most recent k day file, backup the old file(-s and -t options would be ignored)", metavar="int")

parser.add_option("-c", "--conf", dest="conf", default="./conf.json",
                help="config file, setting the backup hdfs-src path and remote target path", metavar="conf.json")

parser.add_option("-t", "--type", dest="backup_type", default=None,
                help="backup data type, should be in XX.XXX.XX format, such as source.tracking.xtx", metavar="AAA.BBB.CCC")

from backup import backup_given_date 



import json
import re
date_re = re.compile("^\d{4}-\d{2}-\d{2}$")
from datetime import datetime,timedelta

CONF_FILE="./conf.json"
def main():
    (options, args) = parser.parse_args()

    #parse backup type from conf
    with open(options.conf) as f:
	conf = json.load(f)
    #print conf
    if not options.backup_type:
	print "backup type not specified, exit"
	exit(-1)
    else:
        backup_type = None
        try:
            key_strings = options.backup_type.split('.')
            expression = "conf[\""+ "\"][\"".join(key_strings)  + "\"]" 
            #print expression
            backup_type = eval(expression)
        except:
            print "backup_type parse error"
        
        if not backup_type:
            print "backup_type error,exit"
            exit(-1)

	if "hdfs-src" not in backup_type or "remote" not in backup_type:
            print "hdfs-src and remote must in the {0}'s config".format(options.backup_type)	
    
    #parse keep 
    if options.keep:
    	keep = int(options.keep)
        if keep <= 0:
            print "-k argument should be greater zero"
            exit(-1)
        else:
            today = datetime.now()
            end_date_str = (today - timedelta(int(keep))).strftime("%Y-%m-%d")
            start_date_str = None
            print "keep the most recent {1} days files backup the file before {0}(including)".format(end_date_str,keep)

    #parse start date and end date	
    else:            
            
        start_date_str = options.start_date_str
        end_date_str = options.end_date_str
        if start_date_str and not date_re.search(start_date_str):
            print "start date format error, should be in %Y-%m-%d format"
            exit(-1)

        if end_date_str and not date_re.search(end_date_str):
            print "end date format error, should be in %Y-%m-%d format"
            exit(-1)
        if not options.start_date_str:
            if not options.end_date_str:
                print "start_date_str and end_date_str is not given, backup all file without date restriction"
            else:
                print "backup the file before {0}(including)".format(end_date_str)
        else:
            if not options.end_date_str:
                print "end_date_str is not given, backup all file start from {0}(including)".format(start_date_str)
            else:
                start_date = datetime.strptime(start_date_str,"%Y-%m-%d")
                end_date = datetime.strptime(end_date_str,"%Y-%m-%d")
                if start_date > end_date:
                    print "start date should be less than or equal the end date"
                    exit(-1)
        
    #print options
    #print args

    #backup_given_date(	
    backup_given_date(options.backup_type, backup_type["hdfs-src"], backup_type["remote"], start_date_str, end_date_str)


if __name__ == "__main__":
    main()
