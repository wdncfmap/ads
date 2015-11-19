# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 16:29:46 2015

@author: MING_YU
"""

import pyodbc
import time
import unicodecsv
from datetime import datetime
from netezza_connection import DBConnFactory
import collections


def run():
    server = 'ADSFRE'
    db = 'MHO_ASIA_BI_DEV'
    schema = 'REP'

    conn_factory = DBConnFactory()
    conn = conn_factory.get_connection(server, db)
    cursor = conn.cursor()

    tbl_dict = collections.OrderedDict()

    tbl_dict['SP_PSYIELD_LOT_HIS_TBL_BATCH'] = ['PSYIELD_LOT_HIS_DENORM_TMP', 'PSYIELD_LOT_HIS_DENORM']
    tbl_dict['SP_PSYIELD_DEFECT_LOG_TBL_BATCH'] = ['PSYIELD_DEFECT_LOG_DENORM_TMP', 'PSYIELD_DEFECT_LOG_DENORM']
    tbl_dict['SP_PSYIELD_LOT_HIS_AGG_TBL_BATCH'] = ['PSYIELD_LOT_HIS_AGG_TMP', 'PSYIELD_LOT_HIS_AGG']
    tbl_dict['SP_PSYIELD_DEFECT_LOG_AGG_TBL_BATCH'] = ['PSYIELD_DEFECT_LOG_AGG_TMP', 'PSYIELD_DEFECT_LOG_AGG']
    tbl_dict['SP_PSYILED_DEFECT_DETAIL_TBL_BATCH'] = ['PSYIELD_DEFECT_DETAIL_DENORM_TMP',
                                                      'PSYIELD_DEFECT_DETAIL_DENORM']
    tbl_dict['SP_PSYIELD_DEFECT_CAUSE_LAP_TBL_BATCH'] = ['PSYIELD_DEFECT_CAUSE_LAP_DENORM_TMP',
                                                         'PSYIELD_DEFECT_CAUSE_LAP_DENORM']
    tbl_dict['SP_PSYIELD_DEFECT_CAUSE_SPC_TBL_BATCH'] = ['PSYIELD_DEFECT_CAUSE_SPC_DENORM_TMP',
                                                         'PSYIELD_DEFECT_CAUSE_SPC_DENORM']

    sp_replace = 'SP_TMP_TABLE_REPLACE'
    step = 1;
    t_begin = time.time()
    for sp, tbl in tbl_dict.items():
        tmp_tbl = tbl[0]
        new_tbl = tbl[1]

        print "Step {}. Get table {}".format(step, tmp_tbl)
        t0 = time.time()
        print '-----Started:', time.strftime('%H:%M:%S')
        sql_sp = "{call " + schema + "." + sp + "()};"
        # print sql_sp
        cursor.execute(sql_sp)
        t1 = time.time()
        print '-----Finished:', time.strftime('%H:%M:%S')
        print 'Time used: ', t1 - t0
        step += 1
        print "Step {}. Replace table {} to {}".format(step, tmp_tbl, new_tbl)
        t0 = time.time()
        print '-----Started:', time.strftime('%H:%M:%S')
        sql_sp = "{call "+schema+"."+sp_replace+"('"+tmp_tbl+"','"+new_tbl+"','"+schema+"','"+schema+"')};"
        # print sql_sp
        cursor.execute(sql_sp)
        t1 = time.time()
        print '-----Finished:', time.strftime('%H:%M:%S')
        print 'Time used: ', t1 - t0
        step += 1
    t_end = time.time()

    print "Total pipeline time:",  time.strftime('%H:%M:%S', time.gmtime(t_end - t_begin))
if __name__ == '__main__':
    run()
