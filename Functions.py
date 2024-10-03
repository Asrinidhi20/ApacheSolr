import pysolr
import csv

SOLR_URL = 'http://localhost:8983/solr'
def get_solr_connection(Employee_Data):
    return pysolr.Solr(f"{SOLR_URL}/{Employee_Data}", always_commit=True)

def createCollection(Employee_Data):
    import subprocess
    command = [
        r"C:\Users\welcome\solr-9.7.0\bin\solr.cmd",
        "create",
        "-c", Employee_Data
    ]
    subprocess.run(command)

def indexData(Employee_Data, excluded_column):
    solr = get_solr_connection(Employee_Data)
    
    with open('employee_data.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data_to_index = []
        
        for row in csv_reader:
            if excluded_column in row:
                del row[excluded_column]
            data_to_index.append(row)
        solr.add(data_to_index)

def searchByColumn(Employee_Data, Department, Department_Name):
    solr = get_solr_connection(Employee_Data)
    query = f"{Department}:{Department_Name}"
    results = solr.search(query)

    return list(results)

def getEmpCount(Employee_Data):
    solr = get_solr_connection(Employee_Data)
    results = solr.search("*:*")
    
    return results.hits

def delEmpById(Employee_Data, Employee_ID):
    solr = get_solr_connection(Employee_Data)
    solr.delete(Employee_ID)

def getDepFacet(Employee_Data):
    solr = get_solr_connection(Employee_Data)
    facet_query = {
        'q': '*:*',
        'facet': 'true',
        'facet.field': 'Department',  
        'facet.limit': -1
    }
    
    results = solr.search(**facet_query)
    
    return results.facets['facet_fields']['Department']

v_nameCollection = 'Hash_Srinidhi'  
v_phoneCollection = 'Hash_1590'  

#createCollection(v_nameCollection)
#createCollection(v_phoneCollection)

#print(f"Employee Count in {v_nameCollection}: {getEmpCount(v_nameCollection)}")

#indexData(v_nameCollection, 'Department')
#indexData(v_phoneCollection, 'Gender')

#delEmpById(v_nameCollection, 'E02003')
#print(f"Employee E02003 deleted from {v_nameCollection}")

#print(f"Employee Count in {v_nameCollection} after deletion: {getEmpCount#(v_nameCollection)}")

#print(f"Search {v_nameCollection} by Department = 'IT':")
#print(searchByColumn(v_nameCollection, 'Department', 'IT'))

#print(f"Search {v_nameCollection} by Gender = 'Male':")
#print(searchByColumn(v_nameCollection, 'Gender', 'Male'))

#print(f"Search {v_phoneCollection} by Department = 'IT':")
#print(searchByColumn(v_phoneCollection, 'Department', 'IT'))

#print(f"Department Facet for {v_nameCollection}:")
#print(getDepFacet(v_nameCollection))

#print(f"Department Facet for {v_phoneCollection}:")
#print(getDepFacet(v_phoneCollection))