import pyjq
from tdigest import TDigest


def stream_read_json(fn):
    """
        Reads one json file until it raises an error due the size of the file
    :param fn: Json file name string
    :return: Generator with all json parts
    """
    import json
    start_pos = 0
    with open(fn, 'r') as f:
        while True:
            try:
                obj = json.load(f)
                yield obj
                return
            except json.JSONDecodeError as e:
                f.seek(start_pos)
                json_str = f.read(e.pos)
                obj = json.loads(json_str)
                start_pos += e.pos
                yield obj


final_digest = TDigest()

json_object = stream_read_json('./metric-result-no-presumed-revenue.json')
for num, json_list in enumerate(json_object):
    num_list = pyjq.all('.[] | .metric', json_list)
    digest = TDigest()
    digest.batch_update(num_list)
    print(digest.percentile(50))
    final_digest = final_digest + digest

print('final: ' + str(final_digest.percentile(25)))
print('final: ' + str(final_digest.percentile(50)))
print('final: ' + str(final_digest.percentile(75)))
print('final: ' + str(final_digest.percentile(90)))
