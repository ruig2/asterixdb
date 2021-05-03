import requests
from urllib.parse import urlparse
import sys

DATA_SCALE = 1000

def load(scale):
    print("load with scale " + str(scale) )

    SQLPP_QUERY = """
    drop dataverse estimator_{scale} if exists;
    create dataverse estimator_{scale};

    use estimator_{scale};

    create type EmployeeType as {{
    id: int,
    salary: int,
    age: int
    }};
    CREATE DATASET Employee(EmployeeType) PRIMARY KEY id;
    CREATE INDEX ageIndex on Employee(age);

    create type EmploymentType as {{
    employment_id: int,
    employee_id: int,
    department_id: int
    }};
    CREATE DATASET Employment(EmploymentType) PRIMARY KEY employment_id;

    LOAD DATASET Employee USING localfs
        (("path"="asterix_nc1:///Users/ray/code/asterix/estimator_data/employee_{scale}.adm"),("format"="adm"));
    LOAD DATASET Employment USING localfs
        (("path"="asterix_nc1:///Users/ray/code/asterix/estimator_data/employment_{scale}.adm"),("format"="adm"));
    """.format(scale = scale)

    # print(SQLPP_QUERY)

    # print(urlparse(SQLPP_QUERY).geturl())

    url = "http://localhost:19002/query/service"
    payload = {
        'statement': SQLPP_QUERY,
        'profile': "timings",
        'logical-plan': True,
        'optimized-logical-plan': True,
        'job': True
    }
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    # print(response.text)

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        DATA_SCALE = int(sys.argv[1])
    load(DATA_SCALE)