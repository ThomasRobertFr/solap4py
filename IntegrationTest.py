# -*-coding:Latin-1 -*

import unittest, solap4py,time,threading, os

class IntegrationTest(unittest.TestCase):

# Real volumes
# 10 MB = 5.9 MB
# 5 MB = 8.2 MB
# 1Mo = 0.939MB
# 100kB = 300kB
# 10kb = 13.2kB
# 1kb = 3.5kB


    nbUsers = 1
    volumeKey = '1MB'
    benchResults = nbUsers*[0]
    threads = nbUsers*[None]
    queries = {'1kB':" { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name2]\", [\"[Zone.Name].[All Zone.Names].[France].[DÉPARTEMENTS D'OUTRE-MER].[Réunion]\"]], \"withProperties\":true, \"granularity\":0}}"}
    queries['10kB']=" { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name2]\", [\"[Zone.Name].[All Zone.Names].[France].[DÉPARTEMENTS D'OUTRE-MER].[Guadeloupe]\"]], \"withProperties\":true, \"granularity\":0}}"
    queries['100kB']=" { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\", [\"[Zone.Name].[All Zone.Names].[France]\"]], \"withProperties\":true, \"granularity\":0}}"
    queries['1MB']=" { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\", \"[Zone.Name].[All Zone.Names].[United Kingdom]\"], \"withProperties\":true, \"granularity\":2}}"
    queries['10MB']=" { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name2]\"], \"withProperties\":true, \"granularity\":0}}"
    queries['5MB']=" { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"], \"withProperties\":true, \"granularity\":0}}"
    

    def test_DTI_06(self):
        query = '{"queryType":"metadata","data":{ "root" :["Traffic"]}}'
        result = solap4py.process(query)
        self.assertEqual(result, '{"error":"OK","data":{"[Traffic]":{"caption":"Traffic"}}}')

    def test_DTI_07(self):
        query = '{"queryType":"metadata","data":{"root":["Traffic", "[Traffic]", "[Zone]", "[Zone.Name]", "[Zone.Name].[Name0]"], "withProperties": false }}'
        result = solap4py.process(query)
        self.assertEqual(result, '{"error":"OK","data":{"[Zone.Name].[All Zone.Names].[Croatia]":{"caption":"Croatia"},"[Zone.Name].[All Zone.Names].[Switzerland]":{"caption":"Switzerland"},"[Zone.Name].[All Zone.Names].[Cyprus]":{"caption":"Cyprus"},"[Zone.Name].[All Zone.Names].[Portugal]":{"caption":"Portugal"},"[Zone.Name].[All Zone.Names].[France]":{"caption":"France"},"[Zone.Name].[All Zone.Names].[Italy]":{"caption":"Italy"},"[Zone.Name].[All Zone.Names].[United Kingdom]":{"caption":"United Kingdom"},"[Zone.Name].[All Zone.Names].[Finland]":{"caption":"Finland"},"[Zone.Name].[All Zone.Names].[Iceland]":{"caption":"Iceland"},"[Zone.Name].[All Zone.Names].[Slovakia]":{"caption":"Slovakia"},"[Zone.Name].[All Zone.Names].[Belgium]":{"caption":"Belgium"},"[Zone.Name].[All Zone.Names].[Luxembourg]":{"caption":"Luxembourg"},"[Zone.Name].[All Zone.Names].[Turkey]":{"caption":"Turkey"},"[Zone.Name].[All Zone.Names].[Norway]":{"caption":"Norway"},"[Zone.Name].[All Zone.Names].[Slovenia]":{"caption":"Slovenia"},"[Zone.Name].[All Zone.Names].[Austria]":{"caption":"Austria"},"[Zone.Name].[All Zone.Names].[Romania]":{"caption":"Romania"},"[Zone.Name].[All Zone.Names].[Czech Republic]":{"caption":"Czech Republic"},"[Zone.Name].[All Zone.Names].[Malta]":{"caption":"Malta"},"[Zone.Name].[All Zone.Names].[Lithuania]":{"caption":"Lithuania"},"[Zone.Name].[All Zone.Names].[Denmark]":{"caption":"Denmark"},"[Zone.Name].[All Zone.Names].[Estonia]":{"caption":"Estonia"},"[Zone.Name].[All Zone.Names].[The former Yugoslav Republic of Macedonia]":{"caption":"The former Yugoslav Republic of Macedonia"},"[Zone.Name].[All Zone.Names].[Hungary]":{"caption":"Hungary"},"[Zone.Name].[All Zone.Names].[Latvia]":{"caption":"Latvia"},"[Zone.Name].[All Zone.Names].[Germany]":{"caption":"Germany"},"[Zone.Name].[All Zone.Names].[Bulgaria]":{"caption":"Bulgaria"},"[Zone.Name].[All Zone.Names].[Sweden]":{"caption":"Sweden"},"[Zone.Name].[All Zone.Names].[Greece]":{"caption":"Greece"},"[Zone.Name].[All Zone.Names].[Netherlands]":{"caption":"Netherlands"},"[Zone.Name].[All Zone.Names].[Liechtenstein]":{"caption":"Liechtenstein"},"[Zone.Name].[All Zone.Names].[Ireland]":{"caption":"Ireland"},"[Zone.Name].[All Zone.Names].[Poland]":{"caption":"Poland"},"[Zone.Name].[All Zone.Names].[Spain]":{"caption":"Spain"}}}')

    def test_DTI_08(self):
        query = '{ "queryType" : "metadata", "data" : { "root" : ["Trafficv"]}}'
        result = solap4py.process(query)
        self.assertEqual(result, '{"error":"BAD_REQUEST","data":"Invalid schema identifier"}')

    def test_DTI_09(self):
        query = '{ "queryType" : "data", "data" : { "from":"[Traffic]", "onColumns":["[Measures].[Max Quantity]"], "onRows":{"[Time]":{"members":["[Time].[All Times].[2000]", "[Time].[All Times].[2003]"], "range":true}}}}'
        result = solap4py.process(query)
        self.assertEqual(result, '{"error":"OK","data":[{"[Measures].[Max Quantity]":311121,"[Time]":"[Time].[All Times].[2000]"},{"[Measures].[Max Quantity]":304574,"[Time]":"[Time].[All Times].[2001]"},{"[Measures].[Max Quantity]":310543,"[Time]":"[Time].[All Times].[2002]"},{"[Measures].[Max Quantity]":315811,"[Time]":"[Time].[All Times].[2003]"}]}')

    def test_DTI_10(self):
        query = '{"queryType":"data","data":{"from":"[Traffic]","onColumns":["[Measures].[Goods Quantity]"],"onRows":{"[Time]":{"members":["[Time].[All Times].[1950]"], "range":false}}}}'
        result = solap4py.process(query)
        self.assertEqual(result, '{"error":"OK","data":[{"[Measures].[Goods Quantity]":0,"[Time]":"[Time].[All Times].[1950]"}]}')
        
    def benchedMethod(self,i):
        query = self.queries[self.volumeKey];
        a = time.time()
        result = solap4py.process(query)
        b = time.time()
        print b-a
        self.benchResults[i] = b-a
        """
        #Measure the volume of the query
        if self.nbUsers == 1 : 
            resultFile = open('../resultOfQuery.txt','w')
            resultFile.write(result)
            resultFile.close()
        """

    def test_DTI_XX(self):
        
        prefix = '{"queryType":"metadata","data":'
        suffix = '}'
        self.queries[self.volumeKey] = prefix + self.queries[self.volumeKey] + suffix  
        
        
        
        for i in range(self.nbUsers):
            self.threads[i] = threading.Thread(None,self.benchedMethod,None, (i,),{} )
            self.threads[i].start()
            
        for i in range(self.nbUsers):
            self.threads[i].join()
            
        directory = "../benchmark/"
        filename = 'benchResults.txt'
        
        if not os.path.exists(directory):
            os.makedirs(directory)
            
        if os.path.isfile(directory+filename):
            my_file = open(directory+filename, "a")
        else:
            my_file = open(directory+filename, "w")
            
        meanBenchResult = sum(self.benchResults)/float(len(self.benchResults))

        my_file.write( str(self.nbUsers) + ' users ' + self.volumeKey + ' : ' + str(meanBenchResult) + '\n' )
        my_file.close()
        
            
    

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(IntegrationTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
