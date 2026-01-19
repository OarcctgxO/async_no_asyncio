import threading
import socket
import prompt_toolkit
import select
import sys


def client(ipv4: str, port: int):
    """Подключается к серверу по введеному адресу и возвращает сокет сервера."""
    server = socket.socket()
    try:
        server.connect((ipv4, port))
        print('Успешное подключение! Можно писать сообщение.')
        return server
    except Exception as er:
        print("Connection:", repr(er))
        server.close()
        return server

def receiver(server_sock: socket.socket):
    """Бесконечно принимает сообщения от сервера, пока подключение не будет разорвано."""
    try:
        while True:
            msg = server_sock.recv(1024).decode()
            msg = msg.strip()
            if not msg:
                raise ConnectionAbortedError('Разрыв соединения')
            prompt_toolkit.print_formatted_text(msg)
    except Exception as er:
        print('Receiver:', repr(er))
    
def inputer(server_sock: socket.socket):
    """
    Бесконечный ввод и отправка сообщений.
    Можно ввести exit для отключения от сервера и завершения работы.
    """
    session = prompt_toolkit.PromptSession()
    while True:
        try:
            msg = session.prompt('> ')
            if msg.lower() == 'exit':
                server_sock.shutdown(socket.SHUT_RDWR)
                server_sock.close()
                raise Exception('Ручное отключение без ошибок.')
            server_sock.send(msg.encode())
        except Exception as er:
            print(repr(er))
            server_sock.close()
            break
    

if __name__ == "__main__":
    try:
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_sock.bind(('0.0.0.0', 0))
        for i in range(3):
            print(f'Отправляю пакет поиска #{i+1}')
            udp_sock.sendto(b"who's the server?", ('255.255.255.255', 5555))
            print('Жду ответа...')
            r, _, _ = select.select([udp_sock], [], [], 2)
            if r:
                msg, addr = udp_sock.recvfrom(1024)
                if msg == b'Hello, I am the server.':
                    ipv4 = addr
                    print('Получен ответ от сервера, подключение...')
                    break
            if i == 2:
                raise ConnectionError('Сервер не найден.')
            else:
                print('Нет ответа, повторяю запрос...')
                continue
                
        server = client(*ipv4)
        threading.Thread(target=inputer, args=(server,), daemon=True).start()
        receiver(server)
    except Exception as err:
        print(repr(err))
        sys.exit(0)