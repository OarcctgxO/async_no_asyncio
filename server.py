import socket
from select import select
from collections import deque
from datetime import datetime


def now() -> str:
    """Текущее время, формат чч:мм:сс"""
    return datetime.now().strftime('%H:%M:%S')

class UnusualAsyncServer:
    def __init__(self, ipv4:str, port: int):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((ipv4, port))
        self.server_sock.listen()
        
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind((ipv4, port))
        self.udp_queue = deque()
        self.udp_gen = self.udp_echo()
        next(self.udp_gen)
        
        self.clients = {}
        self.msg_queue = deque()
        
        self.server = self.server_reader()
        next(self.server)
        
        self.ready_to_read = []
        self.ready_to_write = []
        
        print("Сервер создан.")
        
    def server_reader(self):
        """
        Генератор с бесконечным accept сокета сервера.
        После создания и первого вызова next, каждый следующий next будет вызывать accept.
        Если при этом нет подключения, генератор заблокирует поток до первого подключения.
        Обращаться (то есть next) только если сокет сервера готов к чтению.
        """
        try:
            while True:
                yield
                try:
                    client_sock, client_addr = self.server_sock.accept()
                except Exception:
                    continue
                msg = f"{now()} - ({client_addr[0]}:{client_addr[1]}) --- новое подключение, после выбора ника добавится в чат."
                self.msg_queue.append((msg, client_sock))
                
                reader, writer, queue = self.client_reader(client_sock), self.client_writer(client_sock), deque()
                handler = self.client_handler(client_sock)

                self.clients[client_sock] = {'r': reader, 'w': writer, 'q': queue, 'a': client_addr, 'n': '', 'h': handler}
                
                next(reader)
                next(writer)
                next(handler)
        except Exception as e:
            print(repr(e))
            for sock in self.clients:
                sock.close()
            self.server_sock.close()
    
    def udp_echo(self):
        """
        Генератор, посылающий UDP-ответ на UDP-запрос клиента.
        После создания и первого вызова next, каждый следующий next будет вызывать recvfrom.
        Если при этом в UDP-сокете нет принятого запроса, генератор заблокирует поток.
        Обращаться (то есть next) только если сокет UDP готов к чтению.
        """
        while True:
            yield
            msg_rcv, addr = self.udp_sock.recvfrom(1024)
            if msg_rcv == b"who's the server?":
                msg = 'Hello, I am the server.'
                self.udp_queue.append((msg, addr))
    
    def client_handler(self, sock: socket.socket):
        """
        Генератор-вход для всех новых клиентов.
        Просит клиента написать ник и вписывает его в словарь self.clients.
        WIP: пока не проверяет уникальность ников.
        """
        try:
            msg = 'Сервер принял подключение. Введите ник:'
            self.clients[sock]['q'].append(msg)
            yield
            next(self.clients[sock]['r'])
            nick, _ = self.msg_queue.pop()
            self.clients[sock]['n'] = nick
            msg = f"{now()} - ({self.clients[sock]['a'][0]}:{self.clients[sock]['a'][1]}) --- добавлен в чат с ником: {nick}"
            self.msg_queue.append((msg, sock))
            self.clients[sock]['q'].append(msg)
            yield
        except Exception:
            pass
            
    def client_reader(self, sock: socket.socket):
        """
        Генератор с бесконечным recv сокета клиента.
        После создания и первого вызова next, каждый следующий next будет вызывать recv.
        Если при этом клиент ничего не отправляет, генератор заблокирует поток до первого сообщения.
        Обращаться (то есть next) только если сокет клиента готов к чтению.
        """
        try:
            while True:
                yield
                raw_msg = sock.recv(1024).decode('utf-8', errors='ignore')
                if not raw_msg:
                    return
                if self.clients[sock]['n']:
                    msg = f"{now()} - {self.clients[sock]['n']} -> {raw_msg}"
                    if 'h' in self.clients[sock]:
                        self.clients[sock]['h'].close()
                        del self.clients[sock]['h']
                else:
                    msg = raw_msg
                self.msg_queue.append((msg, sock))
        except Exception:
            pass
            
    
    def client_writer(self, sock: socket.socket):
        """
        Генератор с бесконечным send в сокет клиента. Принимает сообщения в генератор через yield.
        После создания и первого вызова next, следует передавать сообщения через <gen_obj>.send(msg).
        Каждое обращение вызывает socket.send
        Поскольку сокет клиента не ограничен по записи, генератор не должен блокировать поток никогда.
        Для надежности лучше все же проверять доступность записи.
        Обращаться следует, если есть msg, и если сокет клиента готов к записи.
        """
        try:
            while True:
                msg = yield
                sock.send(msg.encode())
        except Exception:
            pass
    
    def client_disconnect_handler(self, sock: socket.socket):
        """Безопасное отключение клиента."""
        try:
            sock.close()
        except socket.error:
            pass
        finally:
            try:
                try:
                    nick = self.clients[sock]['n']
                    self.msg_queue.append((f"{nick} --- отключение", sock))
                finally:
                    del self.clients[sock]
            except Exception:
                pass
                
    def messenger(self):
        """
        Распределяет сообщения от клиентов из общей очереди в личные очереди каждого клиента.
        Не отправляет сообщение отправителю этого сообщения.
        """
        while self.msg_queue:
            msg, ignore_sock = self.msg_queue.pop()
            for sock in self.clients.keys():
                if sock != ignore_sock and self.clients[sock]['n']:
                    self.clients[sock]['q'].append(msg)
            print(msg)
            
    def selector(self):
        """
        Проверяет сокеты клиентов и сервера на доступность для чтения/записи.
        Заполняет списки self.ready_to_read и self.ready_to_write сокетами, готовыми к работе.
        """
        all_socks_read = [*self.clients]
        all_socks_read.append(self.server_sock)
        all_socks_read.append(self.udp_sock)
        
        to_write = [sock for sock in self.clients if self.clients[sock]['q']]
        if self.udp_queue:
            to_write.append(self.udp_sock)
        
        self.ready_to_read, self.ready_to_write, _ = select(all_socks_read, to_write, [])
        
    def loop(self):
        """Главный цикл сервера. Запускает генераторы готовых к работе сокетов, запускает messenger и selector."""
        print("Сервер запущен")
        while True:
            if self.ready_to_read:
                for sock in self.ready_to_read:
                    if sock != self.server_sock:
                        if sock is self.udp_sock:
                            next(self.udp_gen)
                            continue
                        try:
                            if self.clients[sock]['n']:
                                next(self.clients[sock]['r'])
                            else:
                                next(self.clients[sock]['h'])
                        except StopIteration:
                            self.client_disconnect_handler(sock)
                    else:
                        next(self.server)
            self.ready_to_read = []
            
            if self.ready_to_write:
                for sock in self.ready_to_write:
                    if sock is self.udp_sock:
                        udp_msg, udp_addr = self.udp_queue.pop()
                        self.udp_sock.sendto(udp_msg.encode(), udp_addr)
                    elif self.clients[sock]['q']:
                        msg = self.clients[sock]['q'].pop()
                        try:
                            self.clients[sock]['w'].send(msg)
                        except StopIteration:
                            self.client_disconnect_handler(sock)
            self.ready_to_write = []
        
            self.messenger()
            self.selector()


if __name__ == "__main__":
    server = UnusualAsyncServer('0.0.0.0', 5555)
    server.loop()