# udp_common includes C module imports.
# If this import fails, please read enstore-pytest-c-module.md,
# and remember to use `python -m pytest`.
import cleanUDP
import pytest
import select
from mock import patch
from cleanUDP import *

def test_Select():
    # This calls Python's select.select and gets some objects. It calls scrub() on each of them to clean them, which is a method defined on `cleanUDP` class, so I'm guessing there is an assumption that all the objects returned by select.select are `cleanUDP`s.
    pass


def _test_no_ipv(socket_family, peer_addr):
    try:
        socket.socket(socket_family, type=socket.SOCK_DGRAM).sendto(b'', peer_addr)
        return False
    except socket.error:
        return True

class TestCleanUDP:

    @pytest.fixture(params=[
        pytest.param(socket.AF_INET, marks=pytest.mark.skipif(
            _test_no_ipv(socket.AF_INET, ("", 1)),
            reason="No IPv4 available on test host."),
        ),
        pytest.param(socket.AF_INET6, marks=pytest.mark.skipif(
            _test_no_ipv(socket.AF_INET6, ("::", 1, 0, 0)),
            reason="No IPv6 available on test host."),
        ),
    ], ids=['IPv4', 'IPv6'])
    def udp_clean_udp_pair(self, request):
        clean_udp = cleanUDP(request.param, socket.SOCK_DGRAM)
        clean_udp.retry_max = 2
        clean_udp.bind(clean_udp.getsockname())
        udp = socket.socket(request.param, type=socket.SOCK_DGRAM)
        udp.bind(udp.getsockname())
        yield (clean_udp, udp)
        clean_udp.close()
        udp.close()

    def test_init(self, udp_clean_udp_pair):
        for udp_socket in udp_clean_udp_pair:
            port = udp_socket.getsockname()[1]
            assert port != 0
            assert udp_socket.type == socket.SOCK_DGRAM

    def test_scrub(self, udp_clean_udp_pair):
        clean_udp, udp = udp_clean_udp_pair
        # Nothing on socket, should return 0
        assert clean_udp.scrub() == 0
        clean_udp.sendto(b'', clean_udp.getsockname())
        # Read on socket, should return 1
        assert clean_udp.scrub() == 1
        clean_udp.recvfrom(1)
        # Read processed, should return 0
        assert clean_udp.scrub() == 0
        clean_udp.connect(udp.getsockname())
        udp.close()
        clean_udp.send(b'')
        # Error on socket, should clear error and return 0
        scrub = clean_udp.scrub()
        error = clean_udp.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        assert scrub == error == 0

    def test_sendto(self, udp_clean_udp_pair):
        clean_udp, udp = udp_clean_udp_pair
        clean_udp.sendto(b'123', udp.getsockname())
        assert udp.recvfrom(3)[0] == b'123'
        with patch.object(clean_udp.socket, 'sendto',
              side_effect=socket.error('Mock', 'Error')) as mock_sendto:
            with pytest.raises(socket.error):
                clean_udp.sendto(b'123', udp.getsockname())
            assert mock_sendto.call_count == clean_udp.retry_max + 1

    def test_recvfrom(self, udp_clean_udp_pair):
        clean_udp, udp = udp_clean_udp_pair
        udp.sendto(b'123', clean_udp.getsockname())
        assert clean_udp.recvfrom(3, rcv_timeout=0.1)[0] == b'123'
        with patch.object(clean_udp.socket, 'recvfrom',
              side_effect=socket.error('Mock', 'Error')) as mock_recvfrom:
            with patch.object(select, 'select', return_value=(1, 0, 0)):
                print('testing retriess')
                clean_udp.recvfrom(3, rcv_timeout=0.1)
            assert mock_recvfrom.call_count == clean_udp.retry_max

    def test_logerror(self, udp_clean_udp_pair):
        clean_udp, udp = udp_clean_udp_pair
        clean_udp.sendto(b'', clean_udp.getsockname())
        clean_udp.sendto(b'', udp.getsockname())
        with patch.object(Trace, 'log') as mock_trace:
            clean_udp.logerror('TEST', 3)
            for t in ('TEST', 3, clean_udp.getsockname(), udp.getsockname()):
                assert str(t) in str(mock_trace.mock_calls[0])

