import docker
import csv
import os
import time
import argparse

def fetch_docker_stats():
    client = docker.from_env()
    containers = client.containers.list()
    
    data = []
    headers = [
        'Timestamp', 'Container ID', 'Name', 'CPU %', 'Memory Usage', 'Memory Limit', 'Memory %', 
        'Network Input', 'Network Output', 'Block Input', 'Block Output', 'PIDs'
    ]
    
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    
    for container in containers:
        stats = container.stats(stream=False)
        memory_stats = stats['memory_stats']
        cpu_stats = stats['cpu_stats']
        pre_cpu_stats = stats['precpu_stats']
        
        # Calculate CPU usage percentage
        cpu_delta = cpu_stats['cpu_usage']['total_usage'] - pre_cpu_stats['cpu_usage']['total_usage']
        system_delta = cpu_stats['system_cpu_usage'] - pre_cpu_stats['system_cpu_usage']
        cpu_usage = (cpu_delta / system_delta) * len(cpu_stats['cpu_usage']['percpu_usage']) * 100.0
        
        # Memory usage
        memory_usage = memory_stats['usage']
        memory_limit = memory_stats['limit']
        memory_percentage = (memory_usage / memory_limit) * 100.0
        
        # Network I/O
        network_stats = stats.get('networks', {})
        network_input = sum(net['rx_bytes'] for net in network_stats.values())
        network_output = sum(net['tx_bytes'] for net in network_stats.values())
        
        # Block I/O
        block_io_stats = stats['blkio_stats']['io_service_bytes_recursive']
        block_input = sum(bio['value'] for bio in block_io_stats if bio['op'] == 'Read')
        block_output = sum(bio['value'] for bio in block_io_stats if bio['op'] == 'Write')
        
        # PIDs
        pids = stats['pids_stats']['current']
        
        data.append([
            timestamp, container.id[:12], container.name, cpu_usage, memory_usage, memory_limit, 
            memory_percentage, network_input, network_output, block_input, block_output, pids
        ])
    
    return headers, data

def write_to_csv(headers, data, filepath):
    file_exists = os.path.isfile(filepath)
    
    with open(filepath, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(headers)
        writer.writerows(data)

def main(duration, interval):
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, 'docker_stats.csv')
    
    end_time = time.time() + duration
    headers, _ = fetch_docker_stats()
    
    while time.time() < end_time:
        _, data = fetch_docker_stats()
        write_to_csv(headers, data, filepath)
        print(f"Data written to {filepath} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch Docker stats and save to CSV at specified intervals.')
    parser.add_argument('--duration', type=int, default=60, help='Duration in seconds to fetch stats.')
    parser.add_argument('--interval', type=int, default=10, help='Interval in seconds between fetching stats.')
    
    args = parser.parse_args()
    
    main(args.duration, args.interval)
