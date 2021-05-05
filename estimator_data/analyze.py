import json

NUM_REPEAT = 3
NUM_EXPERIMENT = 4

results = []
for num_experiment in range(NUM_EXPERIMENT):
    results.append([])

scale = 1000000
for num_experiment in range(NUM_EXPERIMENT):
    for num_repeat in range(NUM_REPEAT):
        with open('result/result_{}_{}.json'.format(scale, num_repeat)) as f:
            results[num_experiment].append(json.load(f))

    scale *= 2

def lookup_time(name, data):
    for task in data['profile']['joblets'][0]['tasks']:
        if task['partition'] == 1:
            for counter in task['counters']:
                if name.split('@')[0] == counter['name'].split('@')[0]:
                    #print(counter['time'])
                    return(counter['time'])
                elif name.startswith('Index Search'):
                    if (counter['name'].split('/')[-1] == name.split('/')[-1]):
                        # print(counter['time'])
                        return(counter['time'])
    print(name)
    asdf


for task in results[0][0]['profile']['joblets'][0]['tasks']:
    if task['partition'] == 1:
        for counter in task['counters']:
            if (counter['name'] != 'Empty Tuple Source'):
                name = counter['name']
                print('\n' + name, end = '', flush=True)
                for num_experiment in range(NUM_EXPERIMENT):
                    total_time = 0.0
                    # print("\nnum exp: " + str(num_experiment))
                    for num_repeat in range(NUM_REPEAT):
                        run_time = lookup_time(name, results[num_experiment][num_repeat])
                        # print("num repeat: " + str(num_repeat))
                        # print("result: " + str(run_time))
                        total_time += run_time
                    print('\t{avg_time:.4f}'.format(avg_time = total_time / NUM_REPEAT), end = '', flush=True)


print('\nTotal Runtime', end = '', flush=True)
for num_experiment in range(NUM_EXPERIMENT):
    total_time = 0.0
    for num_repeat in range(NUM_REPEAT):
        run_time = float(results[num_experiment][num_repeat]['metrics']['elapsedTime'][:-1])
        #print("num repeat: " + str(num_repeat))
        #print("result: " + str(run_time))
        total_time += run_time
    print('\t{avg_time:.4f}'.format(avg_time = total_time / NUM_REPEAT * 1000), end='', flush=True)