#!/usr/bin/env python3
"""ProxyCommand helper that binds to macOS physical interface (en0) to bypass utun4/Surge."""
import os
import select
import socket
import sys

def main() -> None:
    if len(sys.argv) < 3:
        sys.stderr.write("Usage: ssh_en0_proxy.py <host> <port>\n")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # IP_BOUND_IF = 25 on macOS Darwin
        if_index = socket.if_nametoindex("en0")
        s.setsockopt(socket.IPPROTO_IP, 25, if_index)
    except Exception as e:
        sys.stderr.write(f"Warning: could not bind to en0: {e}\n")

    try:
        s.connect((host, port))
    except Exception as e:
        sys.stderr.write(f"Connection failed: {e}\n")
        sys.exit(1)

    stdin_fd = sys.stdin.fileno()
    stdout_fd = sys.stdout.fileno()
    sock_fd = s.fileno()

    # Use buffer queues and select on writable to properly handle high-throughput flow control
    to_sock = bytearray()
    to_stdout = bytearray()
    stdin_closed = False

    while True:
        rlist = []
        wlist = []

        if not stdin_closed and len(to_sock) < 65536:
            rlist.append(stdin_fd)
        if len(to_stdout) < 65536:
            rlist.append(sock_fd)

        if to_sock:
            wlist.append(sock_fd)
        if to_stdout:
            wlist.append(stdout_fd)

        if not rlist and not wlist:
            break

        readable, writable, _ = select.select(rlist, wlist, [])

        if stdin_fd in readable:
            data = os.read(stdin_fd, 32768)
            if not data:
                stdin_closed = True
                if not to_sock:
                    s.shutdown(socket.SHUT_WR)
            else:
                to_sock.extend(data)

        if sock_fd in readable:
            data = os.read(sock_fd, 32768)
            if not data:
                break
            to_stdout.extend(data)

        if sock_fd in writable and to_sock:
            n = s.send(to_sock)
            del to_sock[:n]
            if not to_sock and stdin_closed:
                s.shutdown(socket.SHUT_WR)

        if stdout_fd in writable and to_stdout:
            n = os.write(stdout_fd, to_stdout)
            del to_stdout[:n]

if __name__ == "__main__":
    main()
