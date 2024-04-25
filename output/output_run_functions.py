import pika
import zlib
import json
import awkward as ak
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

MeV = 0.001
GeV = 1.0
lumi = 10
fraction = 1.0

def connect_to_rabbitmq(host):
    return pika.BlockingConnection(pika.ConnectionParameters(host=host))

def data_expected(samples):
    count = 0
    for value in samples.values():
        if isinstance(value, dict) and "list" in value:
            count += len(value["list"])
    return count

def plot_data(data, samples):
    xmin = 80 * GeV
    xmax = 250 * GeV
    step_size = 5 * GeV

    bin_edges = np.arange(start=xmin, stop=xmax+step_size, step=step_size)
    bin_centres = np.arange(start=xmin+step_size/2, stop=xmax+step_size/2, step=step_size)

    data_x, _ = np.histogram(ak.to_numpy(data['data']['mllll']), bins=bin_edges)
    data_x_errors = np.sqrt(data_x)

    signal_x = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)']['mllll'])
    signal_weights = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)'].totalWeight)
    signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color']

    mc_x, mc_weights, mc_colors, mc_labels = [], [], [], []

    for category, sample in samples.items():
        if category not in ['data', r'Signal ($m_H$ = 125 GeV)']:
            mc_x.append(ak.to_numpy(data[category]['mllll']))
            mc_weights.append(ak.to_numpy(data[category].totalWeight))
            mc_colors.append(sample['color'])
            mc_labels.append(category)

    main_axes = plt.gca()

    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                       fmt='ko', label='Data')

    mc_heights = main_axes.hist(mc_x, bins=bin_edges, weights=mc_weights, stacked=True,
                                color=mc_colors, label=mc_labels)

    mc_x_tot = mc_heights[0][-1]
    mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])

    main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot, weights=signal_weights,
                   color=signal_color, label=r'Signal ($m_H$ = 125 GeV)')

    main_axes.bar(bin_centres, 2*mc_x_err, alpha=0.5, bottom=mc_x_tot-mc_x_err, color='none',
                  hatch="////", width=step_size, label='Stat. Unc.')

    main_axes.set_xlim(left=xmin, right=xmax)
    main_axes.xaxis.set_minor_locator(AutoMinorLocator())
    main_axes.tick_params(which='both', direction='in', top=True, right=True)
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]', fontsize=13, x=1, horizontalalignment='right')
    main_axes.set_ylabel('Events / '+str(step_size)+' GeV', y=1, horizontalalignment='right')
    main_axes.set_ylim(bottom=0, top=np.amax(data_x)*1.6)
    main_axes.yaxis.set_minor_locator(AutoMinorLocator())

    plt.text(0.05, 0.93, 'ATLAS Open Data', transform=main_axes.transAxes, fontsize=13)
    plt.text(0.05, 0.88, 'for education', transform=main_axes.transAxes, style='italic', fontsize=8)
    lumi_used = str(lumi*fraction)
    plt.text(0.05, 0.82, r'$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', transform=main_axes.transAxes)
    plt.text(0.05, 0.76, r'$H \rightarrow ZZ^* \rightarrow 4\ell$', transform=main_axes.transAxes)

    main_axes.legend(frameon=False)

    return

def process_data():
    global data_processed

    # Connect to RabbitMQ server
    connection = connect_to_rabbitmq('rabbitmq')
    channel = connection.channel()

    # Declare the data output queue
    channel.queue_declare(queue='data_output')

    # Import samples & calculate number of expected entries
    samples = load_samples()
    data_expected = data_expected(samples)

    # Initialize data dictionary
    data_full = {category: [] for category in samples}

    # Set up a callback function to handle incoming messages
    def callback(ch, method, properties, body):
        global data_processed
        decompressed_json = zlib.decompress(body)
        data_with_identifier = json.loads(decompressed_json)
        name = data_with_identifier.get('data_name', None)
        data = ak.from_iter(data_with_identifier.get('data', None))

        for category, sample in samples.items():
            if name in sample['list']:
                data_full[category].append(data)

        print(f"Received {name}.")
        data_processed += 1
        print(f"Processed {data_processed} out of {data_expected} datasets.")
        if data_processed == data_expected:
            channel.stop_consuming()

    # Set up a callback function to handle incoming messages
    channel.basic_consume(queue='data_output', auto_ack=True, on_message_callback=callback)

    # Start listening for messages
    channel.start_consuming()

    # Concatenate data lists to awkward arrays
    for category, data_list in data_full.items():
        data_full[category] = ak.concatenate(data_list)

    # Plot data
    plot_data(data_full, samples)

    # Close connection
    connection.close()

if __name__ == "__main__":
    data_processed = 0
    process_data()