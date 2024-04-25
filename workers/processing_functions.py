import pika
import zlib
import json
import awkward as ak
import uproot
import time
import vector
import numpy as np
import infofile

# Variables & Units
MeV = 0.001
GeV = 1.0
lumi = 10
fraction = 1.0
tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"

def connect_to_rabbitmq(host):
    return pika.BlockingConnection(pika.ConnectionParameters(host=host))

def get_xsec_weight(sample):
    info = infofile.infos[sample]
    return (lumi * 1000 * info["xsec"]) / (info["sumw"] * info["red_eff"])

def calc_weight(xsec_weight, events):
    return (
        xsec_weight
        * events.mcWeight
        * events.scaleFactor_PILEUP
        * events.scaleFactor_ELE
        * events.scaleFactor_MUON 
        * events.scaleFactor_LepTRIGGER
    )

def calc_mllll(lep_pt, lep_eta, lep_phi, lep_E):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    return (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV

def cut_lep_charge(lep_charge):
    return lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0

def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    return (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)

def read_file(message):
    message = message.decode('utf-8')
    prefix = message.split()[0]
    sample = message.split()[1]

    path = tuple_path + prefix + sample + ".4lep.root"

    start = time.time()
    data_all = []

    with uproot.open(path + ":mini") as tree:
        numevents = tree.num_entries
        if 'data' not in sample: 
            xsec_weight = get_xsec_weight(sample) 
        for data in tree.iterate(['lep_pt','lep_eta','lep_phi',
                                  'lep_E','lep_charge','lep_type', 
                                  'mcWeight','scaleFactor_PILEUP',
                                  'scaleFactor_ELE','scaleFactor_MUON',
                                  'scaleFactor_LepTRIGGER'], 
                                 library="ak", 
                                 entry_stop=numevents*fraction): 
            nIn = len(data)
            if 'data' not in sample: 
                data['totalWeight'] = calc_weight(xsec_weight, data)

            data = data[~cut_lep_charge(data.lep_charge)]
            data = data[~cut_lep_type(data.lep_type)]
            data['mllll'] = calc_mllll(data.lep_pt, data.lep_eta, data.lep_phi, data.lep_E)
            data_all.append(data)
            elapsed = time.time() - start

    data = ak.concatenate(data_all)
    serialised_data = ak.to_list(data)
    data_with_identifier = {'data_name': sample, 'data': serialised_data}
    json_data = json.dumps(data_with_identifier)

    compressed_data = zlib.compress(json_data.encode('utf-8'))
    return compressed_data

def process_data():
    connection = connect_to_rabbitmq('rabbitmq')
    channel = connection.channel()

    channel.queue_declare(queue='data_processing')
    print('Connected to data_processing queue.')

    channel.queue_declare(queue='data_output')
    print('Connected to data_output queue.')

    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"Worker received: {str(body)}")        

        compressed_json = read_file(body)
        print(f'Processed {str(body)}')

        ch.basic_publish(exchange='', routing_key='data_output', body=compressed_json)

        print(f"Worker sent {str(body)}")

    channel.basic_consume(queue='data_processing', auto_ack=False, on_message_callback=callback)

    print('[*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    process_data()