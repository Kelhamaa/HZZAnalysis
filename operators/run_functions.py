import pika
import json

def establish_rabbitmq_connection(host):
    """Establishes a connection to RabbitMQ server."""
    return pika.BlockingConnection(pika.ConnectionParameters(host=host))

def generate_message_urls(samples):
    """Generate URLs (messages) to be sent to workers."""
    messages = []  # Holds messages to send to workers
    for sample_type, sample_info in samples.items():
        for val in sample_info['list']:
            prefix = "/Data/" if sample_type == 'data' else f"/MC/mc_{val}."
            messages.append(prefix + " " + val)
    return messages

def send_urls_to_rabbitmq(urls, rabbitmq_host):
    """Establishes a connection to RabbitMQ server and sends messages."""
    connection = establish_rabbitmq_connection(rabbitmq_host)
    channel = connection.channel()
    channel.queue_declare(queue='data_processing')  # Declare the data processing queue
    for url in urls:
        channel.basic_publish(exchange='', routing_key='data_processing', body=url)
        print(f"Sent: {url}")
    print("Sent all URLs.")
    connection.close()  # Close connection to RabbitMQ server

if __name__ == "__main__":
    # Retrieve samples and list of URLs
    samples_data = load_samples_from_file()
    message_urls = generate_message_urls(samples_data)

    # Send URLs/messages to RabbitMQ server
    rabbitmq_host = 'rabbitmq'  # Update with your RabbitMQ server host
    send_urls_to_rabbitmq(message_urls, rabbitmq_host)
