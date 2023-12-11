import time
from dataflow import DataFlowThread, GeneratorDataFlowThread

batch_log = []


def batch(df: DataFlowThread):
    try:
        batch_data = ""
        for s in df.iter_input(break_on_end=True):
            batch_data += s
            if "." in s:
                time.sleep(1.0)
                yield batch_data
                batch_data = ""
    except Exception as e:
        print(e)


def gen_strings(df: DataFlowThread):
    for i in range(50):
        for s in ['A', 'B', 'C', 'D', '.']:
            time.sleep(0.1)
            yield s


strs = GeneratorDataFlowThread(gen_strings, max_queue=4)
batches = GeneratorDataFlowThread(batch, input_it=strs, max_queue=2)

#batches.start()
#strs.start()

st = time.time()
idx = 0
for b in batches:
    idx += 1
    if idx > 5:
        break
    elapsed = time.time() - st
    st = time.time()
    print('[%d][%.3f]' % (idx, elapsed), b)
batches.join()