import requests
from urllib.parse import urlparse
import sys

DATA_SCALE = 1000

def execute_query(scale):
    print("execute query with scale " + str(scale))
    SQLPP_QUERY = """
    use estimator_{scale};

    SELECT count(employee.id)
    FROM Employment employment, Employee employee
    WHERE employment.employee_id = employee.id;
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

    return response.text


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        DATA_SCALE = int(sys.argv[1])
    execute_query(DATA_SCALE)