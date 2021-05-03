import generate_data
import load
import execute_query

# SCALE_INIT = 1000000
# NUM_TEST = 4

SCALE_INIT = 10
NUM_TEST = 4

scale = SCALE_INIT

for num_test in range(NUM_TEST):
    print('\ncurrent round: ' + str(num_test))

    generate_data.generate_data(scale)
    load.load(scale)
    for execute_num in range(0, 3):
        file_name = 'result_{scale}_{execute_num}.json'.format(
            scale = scale,
            execute_num = execute_num
        )
        with open(file_name, 'w') as fout:
            fout.write(execute_query.execute_query(scale) + '\n')

    scale *= 2
