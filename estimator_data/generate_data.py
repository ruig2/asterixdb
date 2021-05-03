import sys

DATA_SCALE = 1000

def generate_data(scale):
    print("generating with scale " + str(scale) )
    i = 0
    with open('employee_%d.adm' % scale, 'w') as fout:
        while i < scale:
            fout.write(
                """
                    {"id": %d,
                        "salary": %d,
                        "age": %d
                    }\n\n
                """ % (i, i, i)
            )
            i += 1

    i = 0
    with open('employment_%d.adm' % scale, 'w') as fout:
        while i < scale:
            fout.write(
                """
                    {
                        "employment_id": %d,
                        "employee_id": %d,
                        "department_id": %d
                    }\n\n
                """ % (i, i, i)
            )
            i += 1

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        DATA_SCALE = int(sys.argv[1])
    generate(DATA_SCALE)
