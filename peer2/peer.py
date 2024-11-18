import socket
import json
import os
import threading
import shlex
import hashlib
import math

stop_event = threading.Event()

list_of_peers_online = []

def calculate_piece_hash(piece_data):
    sha1 = hashlib.sha1()
    sha1.update(piece_data)
    return sha1.digest()

def create_pieces_string(pieces):
    hash_pieces = []
    for piece_file_path in pieces:
            with open(piece_file_path, "rb") as piece_file:
                piece_data = piece_file.read()
                piece_hash = calculate_piece_hash(piece_data)
                hash_pieces.append(f"{piece_hash}")
    return hash_pieces

def split_file_into_pieces(file_path, piece_length):
    pieces = []
    with open(file_path, "rb") as file:
        counter = 1
        while True:
            piece_data = file.read(piece_length)
            if not piece_data:
                break
            piece_file_path = f"{file_path}_piece{counter}"
            # piece_file_path = os.path.join("", f"{file_path}_piece{counter}")
            with open(piece_file_path, "wb") as piece_file:
                piece_file.write(piece_data)
            pieces.append(piece_file_path)
            counter += 1
    return pieces

def merge_pieces_into_file(pieces, output_file_path):
    with open(output_file_path, "wb") as output_file:
        for piece_file_path in pieces:
            with open(piece_file_path, "rb") as piece_file:
                piece_data = piece_file.read()
                output_file.write(piece_data)
    print("Got all the parts and created the file",output_file_path)


def get_list_local_files(directory='.'):
    try:
        files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
        return files
    except Exception as e:
        return f"Error: Unable to list files - {e}"

def handle_file_list_request(conn):
    # Retrieve the request data
    try:
        data = conn.recv(4096).decode()
        request = json.loads(data)
        
        if request.get('action') == 'request_file_list':
            # Get the list of local files
            files = get_list_local_files()
            
            # Prepare the response
            response = {'files': files}
            conn.sendall(json.dumps(response).encode() + b'\n')
        else:
            # If the action is not recognized, send an error response
            response = {'error': 'Unknown action'}
            conn.sendall(json.dumps(response).encode() + b'\n')
    except Exception as e:
        response = {'error': f"Error processing request: {e}"}
        conn.sendall(json.dumps(response).encode() + b'\n')
def check_local_files(file_name):
    if not os.path.exists(file_name):
        return False
    else:
        return True
    
def check_local_piece_files(file_name):
    exist_files = []
    directory = os.getcwd()  # Lấy đường dẫn thư mục hiện tại

    for filename in os.listdir(directory):
        if filename.startswith(file_name) and len(filename)>len(file_name):
            exist_files.append(filename)

    if len(exist_files) > 0:
        return exist_files
    else:
        return False

def handle_publish_piece(sock, peers_port, pieces, file_name,file_size,piece_size):
    pieces_hash = create_pieces_string(pieces)
    total_pieces = len(pieces)
    while True:
        print(f"File '{file_name}' has {total_pieces} pieces.")
        user_input_num_piece = input( f"File {file_name} have {pieces}\n piece: {pieces_hash}. \nPlease select num piece in file to publish:" )
        if user_input_num_piece.lower() == "all":
            piece_hash = pieces_hash
            num_order_in_file = list(range(1, total_pieces + 1))
            print("You selected: All pieces")
            for i, hash_value in enumerate(piece_hash, start=1):
                print(f"Number {i}: {hash_value}")
            break
        try:
            num_order_in_file = list(map(int, shlex.split(user_input_num_piece))) 
            if any(num < 1 or num > total_pieces for num in num_order_in_file):
                print(f"Invalid selection. Please select numbers between 1 and {total_pieces}.")
                continue
            piece_hash=[]
            print("You selected: " )
            for i in num_order_in_file:
                index = pieces.index(f"{file_name}_piece{i}")
                piece_hash.append(pieces_hash[index])
                print (f"Number {i} : {pieces_hash[index]}")
            break
        except ValueError:
            print("Invalid input format. Please enter numbers separated by spaces or 'all'.")
    publish_piece_file(sock,peers_port,file_name,file_size, piece_hash,piece_size,num_order_in_file)

def publish_piece_file(sock,peers_port,file_name,file_size, piece_hash,piece_size,num_order_in_file):
    peers_hostname = socket.gethostname()
    command = {
        "action": "publish",
        "peers_port": peers_port,
        "peers_hostname":peers_hostname,
        "file_name":file_name,
        "file_size":file_size,
        "piece_hash":piece_hash,
        "piece_size":piece_size,
        "num_order_in_file":num_order_in_file,
    }
    # shared_piece_files_dir.append(command)
    # print("Publishing piece file") 
    sock.sendall(json.dumps(command).encode() + b'\n')
    # response = sock.recv(4096).decode()
    # print(response)

def request_file_from_peer(peers_ip, peer_port, file_name, piece_hash, num_order_in_file):
    peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        peer_sock.connect((peers_ip, int(peer_port)))
        peer_sock.sendall(json.dumps({'action': 'send_file', 'file_name': file_name, 'piece_hash':piece_hash, 'num_order_in_file':num_order_in_file}).encode() + b'\n')

        # Peer will send the file in chunks of 4096 bytes
        with open(f"{file_name}_piece{num_order_in_file}", 'wb') as f:
            while True:
                data = peer_sock.recv(4096)
                if not data:
                    break
                f.write(data)

        peer_sock.close()
        print(f"Piece of file: {file_name}_piece{num_order_in_file} has been fetched from peer.")
    except Exception as e:
        print(f"An error occurred while connecting to peer at {peers_ip}:{peer_port} - {e}")
    finally:
        peer_sock.close()

def fetch_file(sock,peers_port,file_name, piece_hash, num_order_in_file):
    peers_hostname = socket.gethostname()
    command = {
        "action": "download",
        "peers_port": peers_port,
        "peers_hostname":peers_hostname,
        "file_name":file_name,
        "piece_hash":piece_hash,
        "num_order_in_file":num_order_in_file,
    }
    if check_local_files(file_name):
        print(f"File {file_name} already exists locally.")
        return
    # command = {"action": "fetch", "fname": fname}
    sock.sendall(json.dumps(command).encode() + b'\n')
    response = json.loads(sock.recv(4096).decode())
    if 'peers_info' in response:
        peers_info = response['peers_info']
        host_info_str = "\n".join([f"Number: {peer_info['num_order_in_file'] } {peer_info['peers_hostname']}/{peer_info['peers_ip']}:{peer_info['peers_port']} piece_hash: {peer_info['piece_hash']  } file_size: {peer_info['file_size']  } piece_size: {peer_info['piece_size']  } num_order_in_file: {peer_info['num_order_in_file'] }" for peer_info in peers_info])
        print(f"Hosts with the file {file_name}:\n{host_info_str}")
        if len(peers_info) >= 1:
            chosen_info = input("Enter the number of the host to download from: ")# choose the host, multiple hosts => fix auto choose
            chosen_info_part = shlex.split(chosen_info) 
            # Find the host entry with the chosen IP to get the corresponding lname
            for i in chosen_info_part:
                index = next((j for j, peer_info in enumerate(peers_info) if peer_info.get('num_order_in_file') == i), None)
                if index is not None:
                    request_file_from_peer(peers_info[index]['peers_ip'], peers_info[index]['peers_port'], peers_info[index]['file_name'],peers_info[index]['piece_hash'],peers_info[index]['num_order_in_file'])
                else:
                    print(f"Invalid number entered: {i}")
            if(math.ceil(int(peers_info[0]['file_size'])/int(peers_info[0]['piece_size']))==len(sorted(pieces := check_local_piece_files(file_name)))):
                merge_pieces_into_file(pieces,file_name)
        else:
            print("No hosts have the file.")
    else:
        print("No peers have the file or the response format is incorrect.")

def send_piece_to_client(conn, piece):
    with open(piece, 'rb') as f:
        while True:
            bytes_read = f.read(4096)
            if not bytes_read:
                break
            conn.sendall(bytes_read)

#send file to other peer
def handle_file_request(conn, shared_files_dir):
    try:
        data = conn.recv(4096).decode()
        command = json.loads(data)
        if command['action'] == 'send_file':
            file_name = command['file_name']
            num_order_in_file = command['num_order_in_file']
            file_path = os.path.join(shared_files_dir, f"{file_name}_piece{num_order_in_file}")
            send_piece_to_client(conn, file_path)
    finally:
        conn.close()


#send exit signal to tracker
def send_exit_signal(sock,peers_hostname,peers_port):
    print("exit connection to tracker, hostname:",peers_hostname)
    sock.sendall(json.dumps({'action': 'exit', 'peers_hostname': peers_hostname, 'peers_port':peers_port }).encode() + b'\n')
def start_host_service(port, shared_files_dir):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(('0.0.0.0', port))
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.listen()

    while not stop_event.is_set():
        try:
            server_sock.settimeout(1) 
            conn, addr = server_sock.accept()
            thread = threading.Thread(target=handle_file_request, args=(conn, shared_files_dir))
            thread.start()

        except socket.timeout:
            continue
        except Exception as e:
            break

    server_sock.close()
def handle_req_tracker(sock,peers_hostname,peers_port):
        while  not stop_event.is_set():
            try:
                #handle request from tracker
                # print("handle_req_tracker")
                response_data = sock.recv(4096).decode()
                if not response_data:
                    print("Server closed the connection.")
                    break
                elif response_data == "File list updated successfully.":
                    print("Receive Message: File list updated successfully.")
                else:
                    try:
                        # Chuyển chuỗi JSON thành dictionary
                        response = json.loads(response_data)
                        if response.get('action') == 'ping':
                            sock.sendall(json.dumps({'action': 'ping-reply', 'peers_hostname': peers_hostname, 'peers_port':peers_port }).encode() + b'\n')
                            print("receive ping from tracker")
                            list_of_peers_online=response.get('list-peer-online')
                            if list_of_peers_online:
                                for peer in list_of_peers_online:
                                    if peer != peers_hostname:
                                        print("peer online:",peer)
                                    else:
                                        print("my hostname:",peer)
                        elif response.get('action') == 'request_file_list':
                            handle_file_list_request(sock)
                    except json.JSONDecodeError:
                        print(f"Invalid JSON received: {response_data}")
            except socket.error as e:
                print(f"Socket error in handle_req_tracker: {e}") #In case peer command is "exit"
                break  # Thoát vòng lặp nếu xảy ra lỗi kết nối

    # except Exception as e:
    #     print(f"Error in handle_req_tracker: {e}")
            
def connect_to_server(server_host, server_port, peers_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_host, server_port))
    peers_hostname = socket.gethostname()
    sock.sendall(json.dumps({'action': 'introduce', 'peers_hostname': peers_hostname, 'peers_port':peers_port }).encode() + b'\n')

        # thread_list_file = threading.Thread(target=handle_file_list_request, args=(sock,peers_hostname,peers_port,))
        # thread_list_file.start()
        # thread_reply = threading.Thread(target=handle_ping_request, args=(sock,peers_hostname,peers_port,))
        # thread_reply.start()
    return sock

def main(server_host, server_port, peers_port):
    host_service_thread = threading.Thread(target=start_host_service, args=(peers_port, './'))
    host_service_thread.start()

    # Connect to the server
    sock = connect_to_server(server_host, server_port,peers_port)
    thread_tracker = threading.Thread(target=handle_req_tracker, args=(sock,socket.gethostname(),peers_port,))
    thread_tracker.start()
    try:
        while True:
            user_input = input("Enter command (publish file_name/ download file_name/ exit): ")#addr[0],peers_port, peers_hostname,file_name, piece_hash,num_order_in_file
            command_parts = shlex.split(user_input)
            if len(command_parts) == 2 and command_parts[0].lower() == 'publish':
                _,file_name = command_parts
                if check_local_files(file_name):
                    piece_size = 524288  # 524288 byte = 512KB
                    file_size = os.path.getsize(file_name)
                    pieces = split_file_into_pieces(file_name,piece_size)
                    handle_publish_piece(sock, peers_port, pieces, file_name,file_size,piece_size)
                elif (pieces := check_local_piece_files(file_name)):
                    handle_publish_piece(sock, peers_port, pieces, file_name,file_size,piece_size)
                else:
                    print(f"Local file {file_name}/piece does not exist.")
            elif len(command_parts) == 2 and command_parts[0].lower() == 'download':
                _, file_name = command_parts
                pieces = check_local_piece_files(file_name)
                pieces_hash = [] if not pieces else create_pieces_string(pieces)
                num_order_in_file= [] if not pieces else [item.split("_")[-1][5:] for item in pieces]
                fetch_file(sock,peers_port,file_name, pieces_hash,num_order_in_file)
            elif user_input.lower() == 'exit':
                stop_event.set()  # Stop the host service thread
                send_exit_signal(sock,socket.gethostname(),peers_port)
                sock.close()
                break
            else:
                print("Invalid command.")

    finally:
            sock.close()
            host_service_thread.join()


if __name__ == "__main__":
    # Replace with your server's IP address and port number
    SERVER_HOST = '192.168.56.1'
    SERVER_PORT = 65432
    CLIENT_PORT = 65434
    main(SERVER_HOST, SERVER_PORT,CLIENT_PORT)
