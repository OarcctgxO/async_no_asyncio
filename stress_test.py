import asyncio
import socket


broadcast_addr = ('255.255.255.255', 5555)
udp_request = b"who's the server?"
udp_response = b'Hello, I am the server.'


async def reader_drainer(reader: asyncio.StreamReader):
    try:
        while True:
            if not await reader.read(128):
                return
    except:
        return


async def udp_requester(loop: asyncio.AbstractEventLoop, udp_sock: socket.socket):
    try:
        async with asyncio.timeout(5):
            while True:
                await loop.sock_sendto(udp_sock, udp_request, broadcast_addr)
                wait_task = asyncio.create_task(asyncio.sleep(1))
                while True:
                    get_task = asyncio.create_task(loop.sock_recvfrom(udp_sock, 64))
                    done, _ = await asyncio.wait([get_task, wait_task], return_when="FIRST_COMPLETED")
                    if not get_task in done:
                        get_task.cancel()
                        break
                    else:
                        if udp_response == (await get_task)[0]:
                            return (await get_task)[1]
                        else:
                            continue
    except asyncio.TimeoutError as er:
        new_error = asyncio.TimeoutError('Нет ответа от UDP-сервера')
        raise new_error from er
    finally:
        udp_sock.close()


async def run_bot_client(num: int, barrier_list: list[asyncio.Barrier]):
    #---------------------------------UDP---------------------------------
    loop = asyncio.get_running_loop()
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    addr = await udp_requester(loop, udp_sock)

    #---------------------------------TCP---------------------------------
    reader, writer = await asyncio.open_connection(*addr)
    
    drainer = asyncio.create_task(reader_drainer(reader))
    
    writer.write(f'client_{num}\n'.encode('utf-8'))
    await writer.drain()
    await asyncio.sleep(1)
    await barrier_list[0].wait()
    writer.write(b'Hello! I am online!\n')
    await writer.drain()
    await barrier_list[1].wait()
    writer.write(b'Just a testing message.\n')
    await writer.drain()
    await barrier_list[2].wait()
    writer.write(b'Goodbye!\n')
    await writer.drain()
    
    writer.close()
    await writer.wait_closed()
    drainer.cancel()
    
    return 'Nice'


async def run_multiple_client_bots(num: int):
    barrier_list = [asyncio.Barrier(num) for _ in range(3)]
    bot_clients = [asyncio.create_task(run_bot_client(i, barrier_list)) for i in range(num)]
    
    results = await asyncio.gather(*bot_clients, return_exceptions=True)
    with open('bot_lifes.txt', 'w') as file:
        for res in results:
            file.write(str(res)+'\n')


if __name__ == '__main__':
    asyncio.run(run_multiple_client_bots(500))