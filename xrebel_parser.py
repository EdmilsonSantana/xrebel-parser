# -*- coding: utf-8 -*-
"""
Created on Sun Aug 27 17:11:29 2017

@author: Edmilson Santana
"""

import json
import pandas
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
from collections import Counter

class RequestParser(object):

    REQUEST_INFO_KEY = "sourceInfo"
    REQUEST_TRACE_KEY = "trace"
    REQUEST_TRACES_KEY = "traces"
    REQUEST_QUERIES_KEY = "queries"
    REQUEST_ORM_QUERIES_KEY = "ormQueries"
    REQUEST_TYPE_KEY = "type"
    REQUEST_URL_KEY = "url"
    HTTP_METHOD = "http"
    QUERY_ID_KEY = "id"
    ORM_QUERY_ID_KEY = "ormQueryId"
    QUERY_NUM_ROWS_KEY = "numRowsProcessed"
    QUERY_DURATION_KEY = "duration"
    METHOD_PACKAGE_NAME_KEY = "packageName"
    METHOD_CLASS_NAME_KEY = "className"
    METHOD_NAME_KEY = "methodName"
    ORM_QUERY_ID_KEY = "ormQueryId"
    RAW_QUERY_KEY = "rawQuery"
    TABLE_NAMES_KEY = "tableNames"
    IO_QUERY_ID_LIST_KEY = "ioQueryIdList"
    REQUEST_EXTENSIONS = (".js", ".htm", ".css", ".jsp")
    APP_ROOT_PACKAGE = "br.com.ggas"
    FLUSH_METHOD = "Session.flush"
    
    def __init__(self, request):
        self.request = request
        if self.is_page_request():
            self.query_methods = []
            self.queries_by_id = self.map_queries_by_id()
            self.orm_queries_by_id = self.map_orm_queries_by_id()
        
    def is_page_request(self):
        info = self.request[RequestParser.REQUEST_INFO_KEY]
        request_type = info[RequestParser.REQUEST_TYPE_KEY]
        page_request = False
        if request_type == RequestParser.HTTP_METHOD:
            request_url = info[RequestParser.REQUEST_URL_KEY]
            page_request = not request_url.endswith(
                                RequestParser.REQUEST_EXTENSIONS)
        return page_request
        
    def parse(self):
        data_set = None
        if self.is_page_request():
           stack_trace = [self.request[RequestParser.REQUEST_TRACE_KEY]]
           
           self.fetch_query_methods(stack_trace)
           data_set = pandas.DataFrame(self.parse_query_methods())
        
        return data_set
        
    def fetch_query_methods(self, stack_trace, last_application_trace=None, 
                          last_query_method=None):
        
        for trace in stack_trace:
           
            if self.method_contains_query(trace) and last_query_method:
                self.add_query(last_query_method, trace)
                
            if RequestParser.REQUEST_TRACES_KEY in trace:
              
                query_method = last_query_method
                
                application_trace = last_application_trace
                    
                if self.method_contains_orm(trace) and last_query_method is None:
                    query_method = self.new_query_method(application_trace)
                    self.query_methods.append(query_method)
                
                if self.is_an_application_trace(trace):
                    application_trace = trace
                    
                self.fetch_query_methods(trace[RequestParser.REQUEST_TRACES_KEY], 
                                          application_trace, query_method)
                
            
                
               
    def method_contains_orm(self, trace):
        contains_orm_query = False
        orm_query_id = trace.get(RequestParser.ORM_QUERY_ID_KEY, None)
        if orm_query_id in self.orm_queries_by_id:
            orm_query = self.orm_queries_by_id[orm_query_id]
            contains_orm_query = orm_query[
                    RequestParser.RAW_QUERY_KEY] != RequestParser.FLUSH_METHOD
            
        return contains_orm_query
    
    def is_an_application_trace(self, trace):
        return trace[RequestParser.METHOD_PACKAGE_NAME_KEY].startswith(
                RequestParser.APP_ROOT_PACKAGE)
        
        
    def method_contains_query(self,trace):
        return RequestParser.IO_QUERY_ID_LIST_KEY in trace and trace[
               RequestParser.IO_QUERY_ID_LIST_KEY]
                
    def parse_query_methods(self):
        parsed_query_methods = []
        for query_method in self.query_methods:
            query_method_info = []
            
            queries = query_method[RequestParser.REQUEST_QUERIES_KEY]
            method_name = query_method[RequestParser.METHOD_NAME_KEY]
            
            query_method_info.append(method_name)
            query_method_info.append(len(queries))
            
            for query_metadada in self.get_query_metadata(queries):
                query_method_info.append(query_metadada)
            
            parsed_query_methods.append(query_method_info)
            
        return parsed_query_methods 
            
            
    def get_query_metadata(self, queries):
        sum_num_rows = 0
        sum_query_duration = 0
        accessed_tables  = []
        for query in queries:
            sum_num_rows += float(query[RequestParser.QUERY_NUM_ROWS_KEY])
            sum_query_duration += float(query[RequestParser.QUERY_DURATION_KEY])
            
            for table_name in query[RequestParser.TABLE_NAMES_KEY]:
                if  table_name not in accessed_tables:
                    accessed_tables.append(table_name)
            
            
        return [sum_num_rows, sum_query_duration / 1000000.0, len(accessed_tables)]
        
    def map_queries_by_id(self):
       return self.map_by_id(self.request[RequestParser.REQUEST_QUERIES_KEY],
                             RequestParser.QUERY_ID_KEY)
      
    def map_orm_queries_by_id(self):
        return self.map_by_id(self.request[RequestParser.REQUEST_ORM_QUERIES_KEY],
                              RequestParser.ORM_QUERY_ID_KEY)
     
    def map_by_id(self, elements, id_key):
        elements_by_id = {}
        for element in elements:
            element_id = element[id_key]
            elements_by_id[element_id] = element
        return elements_by_id
    
    def get_query_by_id(self, query_id):
        request_queries = self.request[RequestParser.REQUEST_QUERIES_KEY]
        for query in request_queries:
            if query[RequestParser.QUERY_ID_KEY] == query_id:
                return query
        return None
        
    def new_query_method(self, trace):
        return {RequestParser.METHOD_NAME_KEY: self.get_trace_method(trace), 
                RequestParser.REQUEST_QUERIES_KEY: []}
        
    def add_query(self, query_method, trace):
        for query_id in trace[RequestParser.IO_QUERY_ID_LIST_KEY]:
            query_method[RequestParser.REQUEST_QUERIES_KEY].append(
                        self.queries_by_id[query_id])
        
    def get_trace_method(self, trace):
        return "%s.%s.%s" % (trace[RequestParser.METHOD_PACKAGE_NAME_KEY], 
                             trace[RequestParser.METHOD_CLASS_NAME_KEY],  
                             trace[RequestParser.METHOD_NAME_KEY])
            



def plot(labels, data):
    fig = plt.figure()
    ax = Axes3D(fig, rect=[0, 0, .95, 1], elev=48, azim=134)
    ax.scatter(data[:, 0], data[:, 1], data[:, 2],
               c=labels.astype(np.float), edgecolor='k')

    ax.set_xlabel('Query Number')
    ax.set_ylabel('Number of Rows Processed')
    ax.set_zlabel('Duration')
    ax.dist = 12
    plt.show()

def clustering(data):
    kmeans = KMeans(random_state=0).fit(data)
    
    return kmeans.labels_


def main():
    with open('login.json') as data_file:    
        data = json.load(data_file)

      
    for request in data['requests']:
        parser = RequestParser(request)
        request_data = parser.parse()
        if request_data is not None:           
            request_data.to_csv("result.csv", index=False)
           
            print("Queries: %f" % (sum(request_data.values[:, 1])))
            print("Rows: %f" % (sum(request_data.values[:, 2])))
            print("Duration: %f" % (sum(request_data.values[:, 3])))
            
            labels = clustering(request_data.values[:, 1:])
            clusters = Counter(labels)
            print("Clusters: " + ", ".join(["%d=%d" % (key, clusters[key]) 
                                            for key in clusters.keys()]))
            
            clusters = pandas.DataFrame({"Clusters": labels, 
                                         "Methods":request_data.values[:, 0]})
            clusters.to_csv("clusters.csv", index=False)
            
if __name__ == "__main__":
 
   main()
            
            
        
    
        



