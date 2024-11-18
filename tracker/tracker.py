import logging
import socket
import threading
import json
import psycopg2
from psycopg2 import OperationalError
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Establish a connection to the PostgreSQL database
def create_connection(db_name, user, password, host, port):
    connection = None
    try:
        connection = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("Connection to PostgreSQL DB successful")
        
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

# Replace these values with your database credentials
db_name = "postgres"
user = "postgres"
password = "mysecretpassword"
host = "localhost"  # or your database server's IP
port = "5431"       # default PostgreSQL port

# Test the connection
conn = create_connection(db_name, user, password, host, port)

# conn = psycopg2.connect(dbname="", user="", password="", host="localhost", port="")
cur = conn.cursor()
create_script = '''

        CREATE TABLE IF NOT EXISTS peers (
            peers_ip varchar(255),
            peers_port varchar(255),
            peers_hostname varchar(255),
            file_name varchar(255),
            file_size varchar(255),
            piece_hash varchar(255),
            piece_size varchar(255),
            num_order_in_file varchar(255),
            CONSTRAINT unique_num_order UNIQUE (peers_ip, peers_port, file_name, num_order_in_file)
        );'''

# print("demo create table")
cur.execute(create_script)
conn.commit()

def log_event(message):
    logging.info(message)

def update_client_info(peers_ip,peers_port,peers_hostname,file_name,file_size,piece_hash,piece_size,num_order_in_file):
    # Update the client's file list in the database
    print("Update client info")
    # check peers exist
    # cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    # print("Peers table exists.")
    try:
        for i in range(len(num_order_in_file)):
            cur.execute(
                "INSERT INTO peers (peers_ip,peers_port,peers_hostname,file_name,file_size,piece_hash,piece_size,num_order_in_file) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (peers_ip, peers_port, file_name, num_order_in_file) DO NOTHING;",
                (peers_ip,peers_port,peers_hostname,file_name,file_size,piece_hash[i],piece_size,num_order_in_file[i])
            )
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()  # Rollback the transaction on error
    finally:
        # Optionally close the cursor if it's not needed after this operation
        # cur.close()
        pass

active_connections = {}  
host_files_online = []

def client_handler(conn, addr):
    try:

        while True:
            data = conn.recv(4096).decode()
            # log_event(f"Received data from {addr}: {data}")
            if not data:
                break

            command = json.loads(data)

            peers_ip = addr[0]
            peers_port = command['peers_port']
            peers_hostname = command['peers_hostname']
            file_name = command['file_name'] if 'file_name' in command else ""
            file_size = command['file_size'] if 'file_size' in command else ""
            piece_hash = command['piece_hash'] if 'piece_hash' in command else ""
            piece_size = command['piece_size'] if 'piece_size' in command else ""
            num_order_in_file = command['num_order_in_file'] if 'num_order_in_file' in command else ""

            # when establish connection
            if command.get('action') == 'introduce':
                client_peers_hostname = command.get('peers_hostname')
                print("peerhost name:",peers_hostname)
                active_connections[client_peers_hostname] = conn # store the connection
                host_files_online.append(client_peers_hostname)
                log_event(f"Connection established with {client_peers_hostname}/{peers_ip}:{peers_port})")
                for hostname in host_files_online:
                    print("ping when access",hostname)
                    ping_host(hostname)

            elif command['action'] == 'publish':
                # peers_ip,peers_port,peers_hostname,file_name,piece_hash
                log_event(f"Updating client info in database for hostname: {peers_hostname}/{peers_ip}:{peers_port}")
                update_client_info(peers_ip,peers_port, peers_hostname,file_name,file_size, piece_hash,piece_size,num_order_in_file)  # addr[0] is the IP address
                log_event(f"Database update complete for hostname: {peers_hostname}/{peers_ip}:{peers_port}")
                conn.sendall("File list updated successfully.".encode())

            elif command['action'] == 'download':
                # print("fetch",file_name, num_order_in_file, piece_hash)
                # Query the database for the IP addresses of the clients that have the file
                cur.execute("SELECT * FROM peers WHERE file_name = %s AND num_order_in_file <> ALL (%s) AND piece_hash <> ALL (%s)", (file_name, num_order_in_file, piece_hash))
                results = cur.fetchall()
                if results:
                    # Create a list of dictionaries with 'hostname' and 'ip' keys
                    peers_info = [{'peers_ip': peers_ip, 'peers_port': peers_port, 'peers_hostname': peers_hostname, 'file_name':file_name,'file_size':file_size,'piece_hash':piece_hash,'piece_size':piece_size,'num_order_in_file':num_order_in_file } for peers_ip, peers_port, peers_hostname, file_name,file_size, piece_hash,piece_size, num_order_in_file  in results if peers_hostname in active_connections]
                    conn.sendall(json.dumps({'peers_info': peers_info}).encode())
                else:
                    conn.sendall(json.dumps({'error': 'File not available'}).encode())

            elif command['action'] == 'file_list':
                files = command['files']
                print(f"List of files : {files}")
            elif command['action'] == 'ping-reply':
                log_event(f"Received ping-reply from {peers_hostname}/{peers_ip}:{peers_port}")
            elif command['action'] == 'exit':
                handle_update_list_peer(peers_hostname)
                host_files_online.remove(peers_hostname)
                break

    except Exception as e:
        logging.exception(f"An error occurred while handling client {addr}: {e}")
    finally:
        # if peers_hostname in active_connections:
        #     del active_connections[peers_hostname] 
        if not conn._closed:
            conn.close()
        if peers_hostname in host_files_online:
            host_files_online.remove(peers_hostname)
        log_event(f"Connection with {addr} has been closed.")
        # for hostname in active_connections:
        #     print(hostname)
        #     ping_host(hostname)

def request_file_list_from_client(peers_hostname):
    if peers_hostname in active_connections:
        conn = active_connections[peers_hostname]
        print(active_connections[peers_hostname])
        ip_address, _ = conn.getpeername()
        # print(ip_address)
        peer_port = 65433  
        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_sock.connect((ip_address, peer_port))
        request = {'action': 'request_file_list'}
        peer_sock.sendall(json.dumps(request).encode() + b'\n')
        response = json.loads(peer_sock.recv(4096).decode())
        peer_sock.close()
        if 'files' in response:
            return response['files']
        else:
            return "Error: No file list in response"
    else:
        return "Error: Client not connected"
    
def handle_update_list_peer(peers_hostname):
    active_connections[peers_hostname].close()
    active_connections.pop(peers_hostname)

def discover_files(peers_hostname):
    # Connect to the client and request the file list
    files = request_file_list_from_client(peers_hostname)
    print(f"Files on {peers_hostname}: {files}")

def ping_host(peers_hostname):
    # cur.execute("SELECT address FROM client_files WHERE hostname = %s", (peers_hostname,))
    # results = cur.fetchone()  
    # ip_address = active_connections.get(peers_hostname)#results[0]
    # print(ip_address)
    if peers_hostname in active_connections:
        conn = active_connections[peers_hostname]
        ip_address, _ = conn.getpeername()
        print(ip_address)
        peer_port = 65433 #port client1
        request = {'action': 'ping',"list-peer-online":host_files_online}
        conn.sendall(json.dumps(request).encode() + b'\n')




def server_command_shell():
    while True:
        cmd_input = input("Server command: ")
        cmd_parts = cmd_input.split()
        if cmd_parts:
            action = cmd_parts[0]
            if action == "discover" and len(cmd_parts) == 2:
                hostname = cmd_parts[1]
                thread = threading.Thread(target=discover_files, args=(hostname,))
                thread.start()
            elif action == "ping" and len(cmd_parts) == 2:
                hostname = cmd_parts[1]
                thread = threading.Thread(target=ping_host, args=(hostname,))
                thread.start()
            elif action == "exit":
                # sys.exit(0)         
                break
            else:
                print("Unknown command or incorrect usage.")

def start_server(host='0.0.0.0', port=65432):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    log_event("Server started and is listening for connections.")

    try:
        while True:
            conn, addr = server_socket.accept()
            # host = server_socket.getsockname()
            # log_event(f"Accepted connection from {addr}, hostname is {host}")
            thread = threading.Thread(target=client_handler, args=(conn, addr))
            thread.start()
            log_event(f"Active connections: {threading.active_count() - 1}")
    except KeyboardInterrupt:
        log_event("Server shutdown requested.")
    finally:
        server_socket.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    # print(psycopg2.__version__)

    SERVER_HOST = '192.168.56.1'
    SERVER_PORT = 65432
    # SERVER_HOST='0.0.0.0'
    # Start server in a separate thread
    server_thread = threading.Thread(target=start_server)
    server_thread.start()

    # Start the server command shell in the main thread
    server_command_shell()

    # Signal the server to shutdown
    print("Server shutdown requested.")
    
    sys.exit(0)