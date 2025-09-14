import threading
import socket
import prompt_toolkit


def receiver(server_sock: socket.socket):
    try:
        while True:
            msg = server_sock.recv(1024).decode()
            prompt_toolkit.print_formatted_text(msg)
    except Exception as er:
        print('Receiver:', repr(er))


def client(ipv4: str, port: int):
    server = socket.socket()
    try:
        server.connect((ipv4, port))
        print('Успешное подключение! Можно писать сообщение.')
        return server
    except Exception as er:
        print("Connection:", repr(er))
        server.close()
        return server
    
def inputer(server_sock):
    session = prompt_toolkit.PromptSession()
    while True:
        try:
            msg = session.prompt('> ')
            if msg.lower() == 'exit':
                server_sock.close()
                print('Вы отключились с сервера')
                break
            server_sock.send(msg.encode())
        except Exception as er:
            print(repr(er))
            server_sock.close()
            break
    

def is_valid_ipv4(ipv4:str) -> bool:
    if ipv4 == "localhost":
        return True
    parts = ipv4.split('.')
    if len(parts) != 4:
        return False
    for part in parts:
        if not part.isdigit():
            return False
        num = int(part)
        if num < 0 or num > 255:
            return False
    return True


if __name__ == "__main__":
    while True:
        ipv4 = input("Введите ipv4 адрес сервера: \n")
        if is_valid_ipv4(ipv4):
            break
        else:
            print("Неверный формат!")
    
    try:
        server = client(ipv4, 5555)
        threading.Thread(target=inputer, args=(server,), daemon=True).start()
        receiver(server)
    except Exception as err:
        print(repr(err))