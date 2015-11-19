# -*- coding: utf-8 -*-
"""
Created on Fri Oct 02 12:03:37 2015

@author: MING_YU
"""

import pyodbc
class DBConnFactory:

    def __init__(self):
        self.server_dic = {"ADSFRE": ['wdncfntzprd','mapuser','mapuser'],
              "WDBENG": ['172.16.34.13','BI_ADMIN','B1_ADM1#']}
        self.server = ''
        self.driver = 'NetezzaSQL'
        self.port = '5480'
        self.uid =''
        self.upw = ''
        self.db = ''

    def get_connection(self, connection, database):
        if connection not in self.server_dic.keys():
            print "Server connection failed!"
        else:
            self.server = self.server_dic[connection][0]
            self.uid = self.server_dic[connection][1]
            self.upw = self.server_dic[connection][2]

        self.db = database
        connectionStr = "DRIVER={"+self.driver+"}; SERVER="+self.server+"; PORT="+self.port+"; DATABASE="+self.db+"; UID=" +self.uid+"; PWD="+self.upw+";"

        conn = pyodbc.connect(connectionStr)
        return conn