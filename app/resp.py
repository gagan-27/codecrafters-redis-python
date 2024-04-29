from typing import List
class RespHandler:
    def __init__(self, sock):
        self.buf = bytearray()
        self.sock = sock
        self.msg_len = 0
    # hArray of string, or string(either simple or bulk)
    def extract_by_target(self, target=b"\r\n"):
        while target not in self.buf:
            extra = self.sock.recv(1024)
            self.buf.extend(extra)
        parts = self.buf.split(target, maxsplit=1)
        self.buf = parts[1]
        self.msg_len += len(parts[0]) + 2
        return parts[0]
    def extract_by_length(self, l, eat_crlf=True):
        extra_len = len(b"\r\n") if eat_crlf else 0
        while len(self.buf) < l + extra_len:
            extra = self.sock.recv(1024)
            self.buf.extend(extra)
        v = self.buf[:l]
        self.buf = self.buf[l + extra_len :]
        self.msg_len += l + extra_len
        return v
    def eat_type(self):
        self.buf = self.buf[1:]
        self.msg_len += 1
    def last_msg_len(self):

        return self.msg_len
    def next_msg(self) -> List[str]:
        self.msg_len = 0
        return self._next_msg()

    def _next_msg(self) -> List[str]:
        if len(self.buf) <= 0:
            bs = self.sock.recv(1024)
            if not bs:
                return None
            self.buf.extend(bs)
        t = self.buf[0]
        if t == ord("+"):
            
            self.eat_type()
            v = self.extract_by_target()
            return v
        elif t == ord("$"):
            
            self.eat_type()
            l = int(self.extract_by_target())
            data = self.extract_by_length(l)
            return data
        elif t == ord("*"):
            
            self.eat_type()
            arr_len = int(self.extract_by_target())
            
            data_arr = []
            for _ in range(arr_len):
                v = self._next_msg()
                data_arr.append(v)
            return data_arr
        else:

            raise ValueError(f"Wrong {self.buf.decode()}")