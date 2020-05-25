import datetime
import os
import re
import shutil
import socket
import argparse
import sys
import configparser
import logging


# import pyzabbix

from pyzabbix import ZabbixMetric, ZabbixSender


__version__ = "1.0.2"
root_path = os.path.split(os.path.realpath(__file__))[0]
# os.chdir(root_path)
DAOPS_config = os.path.exists(
    root_path+"\\DAOPS_config.ini") and root_path+"\\DAOPS_config.ini"
# PATS = []  # pats类别的进程的路径 Ex:PATS=[E:\\conf,E:\\conf1\\123]
# FIX = []  # FIX类别的进程的路径
date = datetime.date.today().strftime('%Y%m%d')
hostname = socket.gethostname()
IP = socket.gethostbyname(hostname)  # 获取运行环境本地的IP


def zabbix_report(msg, key='DAOPS', serverIP='192.168.1.2'):
    try:
        packet = [ZabbixMetric(IP, key, msg)]
        zabbixserver = ZabbixSender(serverIP, 10051)
        zabbixserver.send(packet)
    except:
        print(123)


class DA_OPS:

    def __init__(self):
        try:
            config = configparser.ConfigParser()
            config.read(DAOPS_config)
            self.debug = config.getboolean('Debug', 'debug')
            self.zabbixsender = config.getboolean('Zabbix', 'sendmessage')
            self.serverIP = config.get('Zabbix', 'zabbixserverIP')
            self.PATS = config.get('LOG_MOVE', 'PATS').split(';')
            self.FIX = config.get('LOG_MOVE', 'FIX').split(';')

            self.sqlserver_IP = config.get('Sqldata_csv', 'sql_server')
            self.sqlserver_account = config.get('Sqldata_csv', 'account')
            self.sqlserver_password = config.get('Sqldata_csv', 'password')
            self.file_path=config.get('Sqldata_csv','file_path')
            self.stockshare = config.get(
                'Sqldata_csv', 'stockshare').split(';')
            self.stocktrade = config.get(
                'Sqldata_csv', 'stocktrade').split(';')
            self.stocksettle = config.get(
                'Sqldata_csv', 'stocksettle').split(';')
            self.date_range=config.get('Sqldata_csv','date_range')
        except:
            print('config reloaded failed')
            sys.exit(0)
        if self.debug:
            logging.basicConfig(
                filename='DAOPS.log', format='%(asctime)s %(message)s', level=logging.DEBUG)
        else:
            logging.basicConfig(filename='DAOPS.log',
                                format='%(asctime)s %(message)s')


    def LOG_MOVE(self):
        """
        执行日志转移任务的方法
        判断是否有需要执行任务的路径
        全部执行完成后判断是否有执行失败的路径，有则触发发送zabbix报警
        """
        print(self.PATS)
        print(self.FIX)
        if self.PATS:
            for path in self.PATS:
                if os.path.exists(path):
                    try:
                        self._LOG_PATS(path)
                    except:
                        msg = 'Failed: '+path+"  Move wrong"
                        print(msg)
                        zabbix_report(msg, 'LOG_MOVE', self.serverIP)
                else:
                    logging.error('Failed: '+path+"  PATS path exist wrong")
                    msg = 'Failed: '+path+"  path exist wrong"
                    zabbix_report(msg, 'LOG_MOVE', self.serverIP)
        else:
            logging.debug('message: no PATS path exist')
        if self.FIX:
            for path in self.FIX:
                if os.path.exists(path):
                    try:
                        self._FIX_LOG(path)
                        logging.debug('success: '+path)
                    except:
                        msg = 'Failed: '+path+"  Move wrong"
                        print(msg)
                        zabbix_report(msg, 'LOG_MOVE', self.serverIP)

                else:
                    msg = 'Failed'+path+' fix path exist wrong'
                    print(msg)
                    zabbix_report(msg, 'LOG_MOVE', self.serverIP)
        else:
            logging.debug('message: no FIX path exist')
            print(self.FIX)
        logging.debug('Success: LOG_MOVE finished')

    def _LOG_PATS(self, pats_path):
        """
        先遍历路径删除原report文件夹并重新创建
        然后遍历路径，把log类型的文件全部转移到report文件夹内
        重命名文件夹后转移文件夹至log
        """
        #assert os.path.exists(pats_path)
        parttern_log = re.compile(r'\w*\.log\d*', re.I)
        parttern_bak = re.compile(r'\w*\.bak\d*', re.I)
        os.chdir(pats_path)
        try:
            filelist = os.listdir(pats_path)
            if 'report' in filelist:
                shutil.rmtree('report')
                os.mkdir('report')
            else:
                os.mkdir('report')
            for i in os.listdir(pats_path):
                if parttern_log.match(i):
                    # num = re.sub(r'#.*$', "", phone)
                    shutil.move(i, 'report')
                elif parttern_bak.match(i):
                    shutil.move(i, 'report')
            # print(date)
            os.rename(pats_path+'\\report', pats_path+"\\"+date)
            shutil.move(pats_path+"\\"+date, pats_path+'\\log')
            logging.debug('Success: '+pats_path+' PATS LOG MOVE ')
        except:
            print('Failed: '+pats_path+"  Move wrong")
            logging.error('Failed: '+pats_path+"  Move wrong")

    def _FIX_LOG(self, fix_path):
        """
        用于处理FIX类别的进程的日志的方法
        fix日志文件种log格式的文件全部转移至执行日当天的文件夹内
        store日志文件夹整个删除，等到程序重启后重新生成
        """
        #assert os.path.exists(fix_path)
        parttern_log = re.compile(r'\w*\.log\d*', re.I)
        try:
            os.chdir(fix_path+'\\fixlog')
            filelist = os.listdir(fix_path+'\\fixlog')
            if date in filelist:
                shutil.rmtree(date)
                os.mkdir(date)
            else:
                os.mkdir(date)
            for i in os.listdir(os.curdir):
                if parttern_log.match(i):
                    shutil.move(i, date)
            logging.debug("Success:" + fix_path+' fix log move')
        except:
            msg = 'Failed: '+fix_path+' fixLog wrong'
            print(msg)
            logging.error(msg)
        try:
            shutil.rmtree(fix_path+'\\store')
            logging.debug("Success:" + fix_path+' store log move')
        except:
            msg = 'Failed: '+fix_path+' storelog wrong'
            logging.error(msg)
            print(msg)

    def sqldata_csv(self):
        import csv
        import pyodbc
        #import pymssql
        import pandas
        try:
            os.mkdir(self.file_path+'\\'+date)
        except:
            logging.error('Failed: sqldata file exit')

        targetpath=self.file_path+'\\'+date
        sevenday=datetime.date.today()-datetime.timedelta(days=int(self.date_range)) 
        sevendate=sevenday.strftime('%Y%m%d')
        settledate=" where Fdate >='%s' and fdate<'%s'" % (sevendate,date)
        try:
            #conn = pymssql.connect(host=self.sqlserver_IP,user=self.sqlserver_account,password=self.sqlserver_password,database='stockshare',charset='utf8')
            conn = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=stockshare;UID=%s;PWD=%s' % (
                self.sqlserver_IP, self.sqlserver_account, self.sqlserver_password))
            chunksize = 10 ** 5  # tweak this
            #cur = conn.cursor()
            if self.stockshare is not None:
                for table in self.stockshare:
                    if table:
                        sql = 'select * from StockShare.dbo.'+table
                        chunks = pandas.read_sql(sql, conn)
                        dataframe = pandas.DataFrame(chunks)
                        dataframe.to_csv(targetpath+'\\'+'Stockshare_'+table+'.csv', sep=',', index=False)
            
            if self.stocktrade is not None:
                for table in self.stocktrade:
                    if table:
                        sql = 'select * from Stocktrade.dbo.'+table
                        chunks = pandas.read_sql(sql, conn)
                        dataframe = pandas.DataFrame(chunks)
                        dataframe.to_csv(targetpath+'\\'+'Stocktrade_'+table+'.csv', sep=',', index=False)

            if self.stocksettle is not None:
                for table in self.stocksettle:
                    if table:
                        sql = 'select * from stocksettle.dbo.'+table+settledate
                        chunks = pandas.read_sql(sql, conn)
                        dataframe = pandas.DataFrame(chunks)
                        dataframe.to_csv(targetpath+'\\'+'stocksettle_'+table+'.csv', sep=',', index=False)
            conn.close()
        except Exception as e:
            logging.error(e)
           # msg='Failed : export data'
           # logging.error(msg)


if __name__ == '__main__':
    # print("DAOPS:[%s]" % __version__)
    parser = argparse.ArgumentParser(usage='DAOPS [options] COMMAND')
    parser_log = parser.add_argument_group('log move')
    parser_log .add_argument('-LM', '--LOG_MOVE', action="store_true",
                             help='LOG_MOVE方法，检索并处理所有日志路径集合'
                             )

    parser_log .add_argument('-LP', '--LOG_PATS', nargs=1, metavar=('path'), dest='LOG_PATS',
                             help='LOG_PATS方法，可以处理指定路径的PATS格式文件 eg:"--LOG_PATS D:\\test\\pats_communication"')

    parser_log .add_argument('-LF', '--LOG_FIX', nargs=1, metavar=('fix_path'), dest='LOG_FIX',
                             help='LOG_FIX方法，可以处理指定路径的PATS格式文件 eg:"--LOG_PATS D:\\test\\FIX"')

    parser_sql = parser.add_argument_group('sqldata')
    parser_sql.add_argument(
        '-SD', '--SQLDATA', action="store_true", help='根据配置文件内的表生成csv文件')

    if len(sys.argv) == 1:
        DAOPS = DA_OPS()
        DAOPS.sqldata_csv()
        print(parser.print_help())
        sys.exit(0)
    else:
        args, unknown_args = parser.parse_known_args()
        if os.path.exists(DAOPS_config):
            if args.LOG_MOVE:
                """
                try:
                    config = configparser.ConfigParser()
                    config.read(DAOPS_config)
                    zabbix_message = config.getboolean('Zabbix', 'sendmessage')
                    serverIP=config.get('Zabbix','zabbixserverIP')
                    debug = config.getboolean('Debug', 'debug')
                except:
                    print('system init failed')
                    sys.exit(0)
                """
                DAOPS = DA_OPS()
                DAOPS.LOG_MOVE()
            if args.LOG_PATS:
                DAOPS = DA_OPS()
                DAOPS._LOG_PATS(args.LOG_PATS[0])
            if args.LOG_FIX:
                DAOPS = DA_OPS()
                DAOPS._FIX_LOG(args.LOG_FIX[0])
            if unknown_args:
                # logging.ERROR('123')
                print('args wrong')
                print(parser.print_help())
                sys.exit(0)

            if args.SQLDATA:
                DAOPS = DA_OPS()
                DAOPS.sqldata_csv()
        else:
            msg = 'Falied: DAOPS_config load wrong'
            zabbix_report(msg)
            sys.exit(0)
