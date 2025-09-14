import socket
import select
import collections


class UnusualAsyncServer:
    def __init__(self, ipv4:str, port: int):
        self.server_sock = socket.socket()
        self.server_sock.bind((ipv4, port))
        self.server_sock.listen()
        
        self.clients = {}
        self.msg_queue = collections.deque()
        
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
                msg = f"({client_addr[0]}:{client_addr[1]}) --- НОВОЕ ПОДКЛЮЧЕНИЕ"
                self.msg_queue.append((msg, client_sock))\
                
                reader, writer, queue = self.client_reader(client_sock, client_addr), self.client_writer(client_sock, client_addr), collections.deque()
                next(reader)
                next(writer)
                self.clients[client_sock] = (reader, writer, queue, client_addr)
        except Exception as e:
            print(repr(e))
            for sock in self.clients.keys():
                sock.close()
            self.server_sock.close()
            
    def client_reader(self, sock: socket.socket, addr: tuple[str, int]):
        """
        Генератор с бесконечным recv сокета клиента.
        После создания и первого вызова next, каждый следующий next будет вызывать recv.
        Если при этом клиент ничего не отправляет, генератор заблокирует поток до первого сообщения.
        Обращаться (то есть next) только если сокет клиента готов к чтению.
        """
        try:
            while True:
                yield
                raw_msg = sock.recv(1024).decode()
                msg = f"({addr[0]}:{addr[1]}): {raw_msg}"
                self.msg_queue.append((msg, sock))
        except Exception:
            pass
            
    
    def client_writer(self, sock: socket.socket, addr: tuple[str, int]):
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
    
    def client_disconnect_handler(self, sock: socket.socket, addr: tuple[str, int]):
        """Безопасное отключение клиента."""
        try:
            sock.close()
        except socket.error:
            pass
        finally:
            try:
                del self.clients[sock]
                self.msg_queue.append((f"({addr[0]}:{addr[1]}) --- отключился", sock))
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
                if sock != ignore_sock:
                    self.clients[sock][2].append(msg)
            print(msg)
            
    def selector(self):
        """
        Проверяет сокеты клиентов и сервера на доступность для чтения/записи.
        Заполняет списки self.ready_to_read и self.ready_to_write сокетами, готовыми к работе.
        """
        all_socks_read = [*self.clients]
        all_socks_read.append(self.server_sock)
        
        to_write = [sock for sock in self.clients.keys() if self.clients[sock][2]]
        
        self.ready_to_read, self.ready_to_write, _ = select.select(all_socks_read, to_write, [])
        
    def loop(self):
        """Главный цикл сервера. Запускает генераторы готовых к работе сокетов, запускает messenger и selector."""
        print("Сервер запущен")
        while True:
            if self.ready_to_read:
                for sock in self.ready_to_read:
                    if sock != self.server_sock:
                        try:
                            next(self.clients[sock][0])
                        except StopIteration:
                            self.client_disconnect_handler(sock, self.clients[sock][3])
                    else:
                        next(self.server)
            self.ready_to_read = []
            
            if self.ready_to_write:
                for sock in self.ready_to_write:
                    if self.clients[sock][2]:
                        msg = self.clients[sock][2].pop()
                        try:
                            self.clients[sock][1].send(msg)
                        except StopIteration:
                            self.client_disconnect_handler(sock, sock.getpeername())
            self.ready_to_write = []
        
            self.messenger()
            self.selector()

def is_valid_ipv4(ipv4:str) -> bool:
    """Простая проверка введеного адреса."""
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
    
    server = UnusualAsyncServer(ipv4, 5555)
    server.loop()